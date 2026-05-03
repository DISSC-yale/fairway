/**
 * research tool — Parallel multi-agent investigation with synthesis.
 *
 * The tech lead staffs a research team (like debate staffs reviewers).
 * Agents are dispatched in parallel, each writes findings to workDir,
 * then a synthesis agent combines everything into a structured summary.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import { Text } from "@mariozechner/pi-tui";
import { runAgent, getFinalOutput, type UsageStats } from "./dispatch.js";
import { logOperation } from "./log.js";

// ── Helpers ────────────────────────────────────────────────────────

function addUsage(total: UsageStats, add: UsageStats): void {
	total.turns += add.turns;
	total.input += add.input;
	total.output += add.output;
	total.cacheRead += add.cacheRead;
	total.cacheWrite += add.cacheWrite;
	total.cost += add.cost;
}

function emptyUsage(): UsageStats {
	return { turns: 0, input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0, contextTokens: 0 };
}

function slugify(name: string): string {
	return name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "agent";
}

const MAX_CONCURRENCY = 4;

/** Snapshot .md files in workDir before an agent runs, so we can diff after. */
function snapshotMdFiles(workDir: string): Map<string, number> {
	const snap = new Map<string, number>();
	try {
		for (const f of fs.readdirSync(workDir)) {
			if (!f.endsWith(".md")) continue;
			try { snap.set(f, fs.statSync(path.join(workDir, f)).size); } catch { /* skip */ }
		}
	} catch { /* workDir may not exist yet */ }
	return snap;
}

/** Check if new/grown .md files appeared since the snapshot. */
function hasNewOutput(workDir: string, before: Map<string, number>): boolean {
	try {
		for (const f of fs.readdirSync(workDir)) {
			if (!f.endsWith(".md")) continue;
			try {
				const size = fs.statSync(path.join(workDir, f)).size;
				const prev = before.get(f) ?? 0;
				if (size > prev + 100) return true; // file grew or is new and substantial
			} catch { /* skip */ }
		}
	} catch { /* ignore */ }
	return false;
}

const SYNTHESIS_PERSONA = [
	"You are a research synthesizer. Read the research brief and all findings files",
	"in the workDir, then produce a single well-structured synthesis.",
	"",
	"Your output must include:",
	"1. **Executive summary** — 2-3 sentence answer to the research brief",
	"2. **Key findings** — organized by theme, with evidence and source links",
	"3. **Recommendation** — what to do, based on the evidence",
	"4. **Confidence assessment** — how strong is the evidence? What's uncertain?",
	"5. **Unresolved questions** — what couldn't be answered, what needs more work",
	"",
	"Prefer concrete data (numbers, benchmarks, code examples) over vague claims.",
	"Note when sources disagree and explain the conflict.",
	"Write the synthesis to the workDir as synthesis.md.",
].join("\n");

// ── Core: runResearch ──────────────────────────────────────────────

interface AgentSpec {
	persona: string;
	model?: string;
	tools?: string[];
	name?: string;
	thinking?: string;
}

interface ResearchParams {
	brief: string;
	questions?: string[];
	agents: AgentSpec[];
	synthesize?: boolean;
	model?: string;
	workDir: string;
	cwd: string;
	forgeDepth: number;
}

interface ResearchResult {
	synthesis: string;
	agentResults: { name: string; file: string; output: string; failed: boolean }[];
	usage: UsageStats;
}

async function runResearch(
	params: ResearchParams,
	signal?: AbortSignal,
	onUpdate?: (text: string, details: Record<string, unknown>) => void,
): Promise<ResearchResult> {
	if (params.agents.length === 0) {
		throw new Error("At least one research agent is required.");
	}

	// Clean workDir to avoid stale results from prior runs
	fs.rmSync(params.workDir, { recursive: true, force: true });
	fs.mkdirSync(params.workDir, { recursive: true });

	// Write the brief for agents to reference
	const briefPath = path.join(params.workDir, "brief.md");
	const briefContent = [
		"# Research Brief",
		"",
		params.brief,
		...(params.questions && params.questions.length > 0
			? ["", "## Questions", "", ...params.questions.map((q, i) => `${i + 1}. ${q}`)]
			: []),
	].join("\n");
	fs.writeFileSync(briefPath, briefContent, "utf-8");

	const totalUsage = emptyUsage();
	const agentResults: ResearchResult["agentResults"] = [];

	// Assign names (deduplicate)
	const usedNames = new Set<string>();
	const namedAgents = params.agents.map((spec, i) => {
		let name = spec.name ? slugify(spec.name) : `researcher-${i + 1}`;
		while (usedNames.has(name)) name = `${name}-${i}`;
		usedNames.add(name);
		return { ...spec, name };
	});

	// Dispatch all researchers in parallel (with concurrency limit)
	onUpdate?.(`Dispatching ${namedAgents.length} researcher(s)...`, { phase: "investigate" });

	const tasks = namedAgents.map((spec) => {
		const outFile = path.join(params.workDir, `${spec.name}.md`);
		const task = [
			`Research brief: ${params.brief}`,
			...(params.questions && params.questions.length > 0
				? ["", "Questions to answer:", ...params.questions.map((q, i) => `${i + 1}. ${q}`)]
				: []),
			"",
			`Write your findings to ${outFile}.`,
			"Include: key findings with evidence, source URLs/links, confidence level, unresolved questions.",
		].join("\n");
		return { spec, task, outFile };
	});

	// Run with concurrency limit
	for (let i = 0; i < tasks.length; i += MAX_CONCURRENCY) {
		const batch = tasks.slice(i, i + MAX_CONCURRENCY);
		const batchResults = await Promise.allSettled(
			batch.map(async ({ spec, task, outFile }) => {
				const before = snapshotMdFiles(params.workDir);
				let result;
				try {
					result = await runAgent({
						persona: spec.persona,
						task,
						model: spec.model || params.model,
						tools: spec.tools || ["read", "bash"],
						thinking: spec.thinking,
						timeoutMs: 1_800_000, // 30 min wall clock — research takes time, let it run
						// stallMs: default 120s heartbeat — silence means LLM API is hung
						workDir: params.workDir,
						cwd: params.cwd,
						forgeDepth: params.forgeDepth,
						signal,
					});
				} catch (e) {
					// runAgent throws "Agent was aborted" on timeout/stall/signal.
					// Check if the agent wrote useful output before being killed.
					if (hasNewOutput(params.workDir, before)) {
						return { name: spec.name, output: "(partial — agent timed out)", file: outFile, failed: false };
					}
					throw e;
				}
				addUsage(totalUsage, result.usage);
				// Check for non-abort failures (exitCode, stopReason)
				if (result.exitCode !== 0 || result.stopReason === "error") {
					if (hasNewOutput(params.workDir, before)) {
						return { name: spec.name, output: "(partial — agent errored)", file: outFile, failed: false };
					}
					throw new Error(result.errorMessage || result.stderr || `${spec.name} failed (exit ${result.exitCode})`);
				}
				const output = getFinalOutput(result.messages) || "(no output)";
				return { name: spec.name, output, file: outFile, failed: false };
			}),
		);

		for (let j = 0; j < batchResults.length; j++) {
			const r = batchResults[j];
			const name = batch[j].spec.name;
			const outFile = batch[j].outFile;
			if (r.status === "fulfilled") {
				agentResults.push(r.value);
				onUpdate?.(`✓ ${name}`, { phase: "investigate", agent: name });
			} else {
				const errMsg = r.reason instanceof Error ? r.reason.message : String(r.reason);
				onUpdate?.(`✗ ${name}: ${errMsg}`, { phase: "investigate", agent: name });
				fs.writeFileSync(outFile, `# ${name}\n\nFailed: ${errMsg}\n`, "utf-8");
				agentResults.push({ name, file: outFile, output: `Failed: ${errMsg}`, failed: true });
			}
		}
	}

	// Synthesis phase (opt-out with synthesize: false)
	const shouldSynthesize = params.synthesize !== false;
	let synthesis: string;

	if (shouldSynthesize) {
		onUpdate?.("Synthesizing findings...", { phase: "synthesize" });

		const findingsFiles = agentResults.map((r) => r.file).filter((f) => fs.existsSync(f));
		const synthesisTask = [
			`Read the research brief at ${briefPath} and all findings:`,
			...findingsFiles.map((f) => `- ${f}`),
			"",
			`Write synthesis to ${path.join(params.workDir, "synthesis.md")}.`,
		].join("\n");

		const synthesisResult = await runAgent({
			persona: SYNTHESIS_PERSONA,
			task: synthesisTask,
			model: params.model,
			tools: ["read", "bash", "write"],
			workDir: params.workDir,
			cwd: params.cwd,
			forgeDepth: params.forgeDepth,
			signal,
		});
		addUsage(totalUsage, synthesisResult.usage);

		const synthesisPath = path.join(params.workDir, "synthesis.md");
		try {
			synthesis = fs.readFileSync(synthesisPath, "utf-8");
		} catch {
			synthesis = getFinalOutput(synthesisResult.messages) || "(synthesis agent produced no output)";
		}
	} else {
		// No synthesis — concatenate findings
		synthesis = agentResults
			.map((r) => {
				try { return fs.readFileSync(r.file, "utf-8"); } catch { return r.output; }
			})
			.join("\n\n---\n\n");
	}

	return { synthesis, agentResults, usage: totalUsage };
}

// ── Tool Registration ──────────────────────────────────────────────

export function registerResearch(pi: ExtensionAPI, currentDepth: number) {
	pi.registerTool({
		name: "research",
		label: "Research",
		description:
			"Investigate a question with a research team you assemble. " +
			"You define the agents (persona, model, tools) — the tool dispatches them in parallel, " +
			"collects findings, and synthesizes a structured summary. " +
			"Use whenever you need external information — before planning, during review, or for pure research spikes.",
		promptSnippet: "Staff a research team, dispatch in parallel, get a synthesized summary",
		promptGuidelines: [
			"You staff the research team — define each agent's persona, tools, and focus area.",
			"Give each researcher a specific angle: don't send 3 agents with the same vague brief.",
			"Useful tool sets for researchers: ['read', 'bash'] covers web search (curl websearch.kyle.pub), scraping (url-scrape/url-content), gh/fj search, git clone, rg, paper search.",
			"Mention relevant skills in personas: '.agents/skills/web-search/SKILL.md', '.agents/skills/web-scrape/SKILL.md', '.agents/skills/github-issues/SKILL.md', '.agents/skills/paper-search/SKILL.md'.",
			"For code research: 'gh search repos', 'gh search code', shallow clones + rg are powerful. Also works for Codeberg/Gitea via fj or direct API.",
			"Set specific questions — vague briefs get vague results.",
			"Use research() anytime: before planning, after a review flags something, or as the entire deliverable for a spike.",
		],
		parameters: Type.Object({
			brief: Type.String({ description: "Research topic / question to investigate" }),
			questions: Type.Optional(Type.Array(Type.String(), { description: "Specific questions to answer" })),
			agents: Type.Array(
				Type.Object({
					persona: Type.String({ description: "System prompt — who is this researcher and what should they investigate?" }),
					model: Type.Optional(Type.String({ description: "Model ID (default: auto)" })),
					tools: Type.Optional(Type.Array(Type.String(), { description: "Tools to enable. Default: ['read', 'bash']" })),
					name: Type.Optional(Type.String({ description: "Short name for file naming, e.g. 'web-benchmarks', 'github-issues'" })),
					thinking: Type.Optional(Type.String({ description: "Thinking level" })),
				}),
				{ description: "Research agents to dispatch in parallel", minItems: 1 },
			),
			synthesize: Type.Optional(Type.Boolean({ description: "Run a synthesis agent after research. Default: true", default: true })),
			model: Type.Optional(Type.String({ description: "Default model for agents that don't specify one" })),
			workDir: Type.String({ description: "Directory for research output files" }),
		}),

		async execute(_toolCallId, params, signal, onUpdate, ctx) {
			const startTime = Date.now();

			const result = await runResearch(
				{
					brief: params.brief,
					questions: params.questions,
					agents: params.agents,
					synthesize: params.synthesize,
					model: params.model,
					workDir: params.workDir,
					cwd: ctx.cwd,
					forgeDepth: currentDepth,
				},
				signal,
				onUpdate
					? (text, details) => {
							onUpdate({
								content: [{ type: "text", text }],
								details,
							});
						}
					: undefined,
			);

			const duration = Date.now() - startTime;
			const succeeded = result.agentResults.filter((r) => !r.failed).length;
			const failed = result.agentResults.filter((r) => r.failed).length;

			logOperation(params.workDir, {
				tool: "research",
				timestamp: new Date().toISOString(),
				params: { brief: params.brief, questions: params.questions, agents: params.agents.length, workDir: params.workDir },
				result: `${succeeded} succeeded, ${failed} failed, synthesis ${params.synthesize !== false ? "yes" : "skipped"}`,
				cost: result.usage.cost,
				duration,
			});

			return {
				content: [{ type: "text", text: result.synthesis }],
				details: { agentResults: result.agentResults, usage: result.usage },
			};
		},

		renderCall(args, theme) {
			const brief = args.brief?.length > 60 ? `${args.brief.slice(0, 60)}...` : (args.brief || "...");
			const count = args.agents?.length ?? 0;
			let text = theme.fg("toolTitle", theme.bold("research"));
			text += theme.fg("muted", ` (${count} agent${count !== 1 ? "s" : ""})`);
			text += `\n  ${theme.fg("accent", "brief:")} ${theme.fg("dim", brief)}`;
			if (args.agents?.length) {
				for (const a of args.agents.slice(0, 3)) {
					const name = a.name || a.persona?.slice(0, 40) || "...";
					text += `\n  ${theme.fg("accent", "•")} ${theme.fg("dim", name)}`;
				}
				if (args.agents.length > 3) text += `\n  ${theme.fg("dim", `  +${args.agents.length - 3} more`)}`;
			}
			return new Text(text, 0, 0);
		},

		renderResult(result, _options, theme) {
			const details = result.details as { agentResults?: { name: string; failed: boolean }[]; usage?: UsageStats; phase?: string } | undefined;

			// Intermediate update (no agentResults yet) — show progress text
			if (!details?.agentResults) {
				const progressText = result.content[0]?.type === "text" ? result.content[0].text : "researching...";
				return new Text(`${theme.fg("muted", "⠋")} ${theme.fg("toolTitle", theme.bold("research"))} ${theme.fg("dim", progressText)}`, 0, 0);
			}

			const agents = details.agentResults;
			const succeeded = agents.filter((r) => !r.failed).length;
			const failed = agents.filter((r) => r.failed).length;
			const cost = details?.usage?.cost ?? 0;
			const costStr = cost > 0 ? ` $${cost.toFixed(4)}` : "";

			const icon = failed === 0 ? theme.fg("success", "✓") : theme.fg("warning", "⚠");
			let summary = `${succeeded} ok`;
			if (failed > 0) summary += `, ${failed} failed`;

			const preview = result.content[0]?.type === "text"
				? (result.content[0].text.length > 300 ? `${result.content[0].text.slice(0, 300)}...` : result.content[0].text)
				: "(no output)";

			let text = `${icon} ${theme.fg("toolTitle", theme.bold("research"))} ${theme.fg("dim", summary)}${theme.fg("dim", costStr)}`;
			text += `\n${theme.fg("toolOutput", preview)}`;

			return new Text(text, 0, 0);
		},
	});
}
