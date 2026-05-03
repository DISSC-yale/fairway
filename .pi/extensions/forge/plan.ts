/**
 * plan command — Autonomous draft→review→fix loop for implementation plans.
 *
 * Dispatches a drafter agent, runs a review debate, and iterates until
 * reviewers converge (LGTM) or max cycles are reached. Reuses forge's
 * dispatch and debate infrastructure.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { runAgent, getFinalOutput, type UsageStats } from "./dispatch.js";
import { runDebate, type AgentSpec, type DebateResult } from "./debate.js";
import { logOperation } from "./log.js";
import { hasProvider } from "./providers.js";

// ── Config ────────────────────────────────────────────────────────

interface PlanConfig {
	drafter: string;
	reviewers: string[];
	maxCycles: number;
	scout: boolean;
}

/** Pick a cheap model for utility tasks (issue fetching, etc). */
function utilityModel(): string {
	if (hasProvider("anthropic")) return "anthropic/claude-haiku-4-5-20251001";
	if (hasProvider("google")) return "google/gemini-2.5-flash";
	if (hasProvider("openai")) return "openai/gpt-4o-mini";
	return "anthropic/claude-haiku-4-5-20251001";
}

function buildDefaultConfig(): PlanConfig {
	const hasAnthropic = hasProvider("anthropic");
	const hasGoogle = hasProvider("google");

	let drafter: string;
	let reviewers: string[];

	if (hasAnthropic && hasGoogle) {
		drafter = "anthropic/claude-sonnet-4-6";
		reviewers = ["google/gemini-2.5-pro", "anthropic/claude-opus-4-6"];
	} else if (hasGoogle) {
		// Gemini-only: use two distinct Gemini models for reviewer diversity.
		drafter = "google/gemini-2.5-pro";
		reviewers = ["google/gemini-2.5-pro", "google/gemini-2.5-flash"];
	} else if (hasAnthropic) {
		drafter = "anthropic/claude-sonnet-4-6";
		reviewers = ["anthropic/claude-opus-4-6", "anthropic/claude-sonnet-4-6"];
	} else {
		// Last resort — let dispatch surface the credential error.
		drafter = "anthropic/claude-sonnet-4-6";
		reviewers = ["anthropic/claude-opus-4-6", "anthropic/claude-sonnet-4-6"];
	}

	return {
		drafter,
		reviewers,
		maxCycles: 4,
		scout: true,
	};
}

/** Model fallback chain — adapts to available providers. */
function buildFallback(): Record<string, string> {
	const fb: Record<string, string> = {};
	const hasAnthropic = hasProvider("anthropic");
	const hasGoogle = hasProvider("google");

	if (hasAnthropic && hasGoogle) {
		fb.anthropic = "google/gemini-2.5-pro";
		fb.google = "anthropic/claude-sonnet-4-6";
	} else if (hasGoogle) {
		// Gemini-only: fall back within Google (pro → flash).
		fb.google = "google/gemini-2.5-flash";
		fb.anthropic = "google/gemini-2.5-pro";
	} else if (hasAnthropic) {
		fb.anthropic = "anthropic/claude-sonnet-4-6";
	}
	return fb;
}

function providerOf(model: string): string {
	return model.split("/")[0] || "unknown";
}

function fallbackFor(model: string): string | undefined {
	return buildFallback()[providerOf(model)];
}

interface ParsedObjective {
	config: PlanConfig;
	objective: string;
}

/**
 * Parse objective text, extracting optional YAML frontmatter config.
 * Frontmatter is delimited by --- lines. All fields optional.
 */
function parseObjective(raw: string): ParsedObjective {
	const defaults = buildDefaultConfig();
	const config = { ...defaults };
	let objective = raw.trim();

	const fmMatch = objective.match(/^---\s*\n([\s\S]*?)\n---\s*\n([\s\S]*)$/);
	if (fmMatch) {
		const yaml = fmMatch[1];
		objective = fmMatch[2].trim();

		let hasReviewerOverride = false;
		// Simple YAML parsing — no dependency needed for flat key/value + arrays
		for (const line of yaml.split("\n")) {
			const kv = line.match(/^(\w+)\s*:\s*(.+)$/);
			if (kv) {
				const [, key, val] = kv;
				if (key === "drafter") config.drafter = val.trim();
				if (key === "max_cycles") config.maxCycles = parseInt(val.trim(), 10) || defaults.maxCycles;
				if (key === "scout") config.scout = val.trim().toLowerCase() !== "false";
			}
			// Array items (reviewers)
			const arrItem = line.match(/^\s*-\s+(.+)$/);
			if (arrItem && yaml.includes("reviewers:")) {
				if (!hasReviewerOverride) {
					config.reviewers = [];
					hasReviewerOverride = true;
				}
				config.reviewers.push(arrItem[1].trim());
			}
		}
	}

	return { config, objective };
}

// ── Helpers ───────────────────────────────────────────────────────

function slugify(text: string): string {
	return text
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/-+/g, "-")
		.replace(/^-|-$/g, "")
		.slice(0, 40) || "plan";
}

function planWorkDir(objective: string): string {
	return `/tmp/forge/plan-${slugify(objective)}`;
}

// ── Personas ──────────────────────────────────────────────────────

const SCOUT_PERSONA = `You are a codebase scout. Read the project structure, key files, and recent git history. Produce a concise context summary covering:
- Project purpose and tech stack
- Directory structure and key entry points
- Relevant data sources, schemas, or pipelines
- Recent changes and active work
- Constraints or patterns that a planner should know about

Be factual. Do not propose solutions.`;

const DRAFTER_PERSONA = `You are an implementation planner. Given an objective and codebase context, produce a detailed implementation plan.

Structure your plan as:

## Objective
One sentence restating what we're building and why.

## Context
Key facts about the codebase, data, and constraints discovered during scouting.

## Approach
The chosen strategy and why alternatives were rejected.

## Steps
Numbered steps with:
- What to do (specific files, functions, commands)
- Dependencies (what must complete first)
- Acceptance criteria (how to verify this step worked)

## Risks
What could go wrong, and mitigations.

## Test Plan
How to validate the complete implementation works.

When revising based on reviewer feedback, address every point explicitly.
Do not drop steps that weren't criticized — only add/modify what reviewers flagged.`;

const FEASIBILITY_REVIEWER_PERSONA = `You are a pragmatic engineer reviewing an implementation plan.
Focus on: Are the steps actually doable? Are dependencies correct?
Are there missing steps? Will the test plan catch real failures?
Be specific — cite step numbers when flagging issues.
End with exactly: LGTM: True or LGTM: False`;

const DOMAIN_REVIEWER_PERSONA = `You are a domain expert reviewing an implementation plan.
Focus on: Does the approach make sense for this problem domain?
Are there better alternatives? Are assumptions valid?
Are risks adequately identified? Is scope appropriate?
End with exactly: LGTM: True or LGTM: False`;

function reviewerAgents(models: string[]): AgentSpec[] {
	const personas = [FEASIBILITY_REVIEWER_PERSONA, DOMAIN_REVIEWER_PERSONA];
	return models.map((model, i) => ({
		persona: personas[i % personas.length],
		model,
		tools: ["read", "bash"],
		name: i === 0 ? "feasibility" : "domain",
	}));
}

// ── Core: runPlanLoop ─────────────────────────────────────────────

interface PlanResult {
	converged: boolean;
	cycles: number;
	reason: string;
	planFile?: string;
	consensusFile?: string;
	workDir: string;
	totalUsage: UsageStats;
}

function emptyUsage(): UsageStats {
	return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0, contextTokens: 0, turns: 0 };
}

function addUsage(total: UsageStats, add: UsageStats): void {
	total.input += add.input;
	total.output += add.output;
	total.cacheRead += add.cacheRead;
	total.cacheWrite += add.cacheWrite;
	total.cost += add.cost;
	total.turns += add.turns;
}

async function runPlanLoop(
	objective: string,
	config: PlanConfig,
	cwd: string,
	forgeDepth: number,
	signal?: AbortSignal,
	onStatus?: (msg: string) => void,
	isCancelled?: () => boolean,
): Promise<PlanResult> {
	const workDir = planWorkDir(objective);
	fs.mkdirSync(workDir, { recursive: true });

	const totalUsage = emptyUsage();
	const status = (msg: string) => {
		onStatus?.(msg);
		process.stderr.write(`[forge/plan] ${msg}\n`);
	};

	// ── Phase 1: Scout ────────────────────────────────────────────
	let scoutContext = "";
	if (config.scout) {
		status("Scouting codebase...");
		try {
			const scoutResult = await runAgent({
				persona: SCOUT_PERSONA,
				task: `Scout this codebase. Write your findings to ${workDir}/scout/context.md`,
				model: config.drafter, // reuse drafter model for scout
				tools: ["read", "bash"],
				workDir: path.join(workDir, "scout"),
				cwd,
				forgeDepth,
				signal,
			});
			addUsage(totalUsage, scoutResult.usage);
			const scoutFile = path.join(workDir, "scout", "context.md");
			if (fs.existsSync(scoutFile)) {
				scoutContext = fs.readFileSync(scoutFile, "utf-8");
			} else {
				scoutContext = getFinalOutput(scoutResult.messages);
			}
			status("Scout complete.");
		} catch (err) {
			status(`Scout failed (${err}), continuing without context.`);
		}
	}

	// ── Phase 2-4: Draft → Review → Fix loop ─────────────────────
	let lastDraftFile = "";

	for (let cycle = 1; cycle <= config.maxCycles; cycle++) {
		if (isCancelled?.()) {
			return {
				converged: false,
				cycles: cycle - 1,
				reason: "cancelled",
				planFile: lastDraftFile ? path.basename(lastDraftFile) : undefined,
				workDir,
				totalUsage,
			};
		}

		// ── Draft ─────────────────────────────────────────────────
		status(`Cycle ${cycle}/${config.maxCycles}: drafting plan...`);

		let drafterTask = `Write an implementation plan for the following objective.\n\n## Objective\n${objective}\n`;
		if (scoutContext) {
			drafterTask += `\n## Codebase Context\n${scoutContext}\n`;
		}

		// On revision cycles, include reviewer feedback
		if (cycle > 1) {
			const prevReviewDir = path.join(workDir, `plan-review-${cycle - 1}`);
			const feedbackFiles = fs.existsSync(prevReviewDir)
				? fs.readdirSync(prevReviewDir).filter(f => f.startsWith("round-") && f.endsWith(".md"))
				: [];
			if (feedbackFiles.length > 0) {
				drafterTask += `\n## Reviewer Feedback (address every point)\nRead the review files in ${prevReviewDir}/ for detailed feedback:\n`;
				for (const f of feedbackFiles) {
					drafterTask += `- ${path.join(prevReviewDir, f)}\n`;
				}
			}
			if (lastDraftFile) {
				drafterTask += `\nYour previous draft is at ${lastDraftFile} — revise it, don't start from scratch.\n`;
			}
		}

		const draftFile = path.join(workDir, `draft-${cycle}.md`);
		let drafterModel = config.drafter;

		let draftOutput: string;
		try {
			const draftResult = await runAgent({
				persona: DRAFTER_PERSONA,
				task: drafterTask,
				model: drafterModel,
				tools: ["read", "bash"],
				workDir: workDir,
				cwd,
				forgeDepth,
				signal,
			});
			addUsage(totalUsage, draftResult.usage);
			draftOutput = getFinalOutput(draftResult.messages);
		} catch (err) {
			// Token exhaustion — try fallback model
			const fb = fallbackFor(drafterModel);
			if (fb) {
				status(`Drafter (${drafterModel}) failed: ${err}. Retrying with ${fb}...`);
				drafterModel = fb;
				const draftResult = await runAgent({
					persona: DRAFTER_PERSONA,
					task: drafterTask,
					model: drafterModel,
					tools: ["read", "bash"],
					workDir: workDir,
					cwd,
					forgeDepth,
					signal,
				});
				addUsage(totalUsage, draftResult.usage);
				draftOutput = getFinalOutput(draftResult.messages);
			} else {
				status(`Drafter failed and no fallback available. Saving state.`);
				return {
					converged: false,
					cycles: cycle,
					reason: `drafter_failed: ${err}`,
					workDir,
					totalUsage,
				};
			}
		}

		if (!draftOutput.trim()) {
			status("Drafter produced empty output. Retrying once...");
			try {
				const retry = await runAgent({
					persona: DRAFTER_PERSONA,
					task: drafterTask,
					model: drafterModel,
					tools: ["read", "bash"],
					workDir: workDir,
					cwd,
					forgeDepth,
					signal,
				});
				addUsage(totalUsage, retry.usage);
				draftOutput = getFinalOutput(retry.messages);
			} catch {
				// give up
			}
			if (!draftOutput.trim()) {
				return {
					converged: false,
					cycles: cycle,
					reason: "drafter_empty_output",
					workDir,
					totalUsage,
				};
			}
		}

		fs.writeFileSync(draftFile, draftOutput, "utf-8");
		lastDraftFile = draftFile;
		status(`Draft ${cycle} written to ${draftFile}`);

		// ── Review debate ─────────────────────────────────────────
		status(`Cycle ${cycle}/${config.maxCycles}: reviewing plan...`);

		const reviewDir = path.join(workDir, `plan-review-${cycle}`);

		try {
			const debateResult = await runDebate(
				{
					topic: `Review implementation plan: ${objective}`,
					contextFiles: [draftFile],
					agents: reviewerAgents(config.reviewers),
					topology: "round-table",
					convergence: "LGTM",
					maxRounds: 2,
					workDir: reviewDir,
					cwd,
					forgeDepth,
				},
				signal,
			);
			addUsage(totalUsage, debateResult.result.totalUsage);

			if (debateResult.result.converged) {
				// ── Converged! Write final outputs ────────────────
				const planFile = path.join(workDir, "PLAN.md");
				fs.copyFileSync(draftFile, planFile);

				const consensusDir = path.join(workDir, "plan-review");
				fs.mkdirSync(consensusDir, { recursive: true });
				const consensusFile = path.join(consensusDir, "consensus.md");
				if (debateResult.result.consensusFile) {
					fs.copyFileSync(
						path.join(reviewDir, debateResult.result.consensusFile),
						consensusFile,
					);
				} else {
					fs.writeFileSync(consensusFile, `# Plan Consensus\n\nConverged after ${cycle} cycle(s).\n`, "utf-8");
				}

				const costStr = totalUsage.cost > 0 ? ` ($${totalUsage.cost.toFixed(4)})` : "";
				status(`Converged after ${cycle} cycle(s)${costStr}`);

				return {
					converged: true,
					cycles: cycle,
					reason: "converged",
					planFile: "PLAN.md",
					consensusFile: "plan-review/consensus.md",
					workDir,
					totalUsage,
				};
			}

			status(`Review ${cycle} did not converge. ${cycle < config.maxCycles ? "Revising..." : "Max cycles reached."}`);
		} catch (err) {
			// Debate failed — try fallback reviewer models
			const fbModels = config.reviewers.map(m => fallbackFor(m) || m);
			if (fbModels.some((m, i) => m !== config.reviewers[i])) {
				status(`Review debate failed: ${err}. Retrying with fallback models...`);
				try {
					const retryResult = await runDebate(
						{
							topic: `Review implementation plan: ${objective}`,
							contextFiles: [draftFile],
							agents: reviewerAgents(fbModels),
							topology: "round-table",
							convergence: "LGTM",
							maxRounds: 2,
							workDir: reviewDir + "-retry",
							cwd,
							forgeDepth,
						},
						signal,
					);
					addUsage(totalUsage, retryResult.result.totalUsage);

					if (retryResult.result.converged) {
						const planFile = path.join(workDir, "PLAN.md");
						fs.copyFileSync(draftFile, planFile);
						const consensusDir = path.join(workDir, "plan-review");
						fs.mkdirSync(consensusDir, { recursive: true });
						fs.writeFileSync(
							path.join(consensusDir, "consensus.md"),
							`# Plan Consensus\n\nConverged after ${cycle} cycle(s) (fallback reviewers).\n`,
							"utf-8",
						);
						return {
							converged: true,
							cycles: cycle,
							reason: "converged_with_fallback",
							planFile: "PLAN.md",
							consensusFile: "plan-review/consensus.md",
							workDir,
							totalUsage,
						};
					}
				} catch (retryErr) {
					status(`Fallback review also failed: ${retryErr}`);
				}
			} else {
				status(`Review debate failed and no fallback available: ${err}`);
			}
		}
	}

	return {
		converged: false,
		cycles: config.maxCycles,
		reason: "max_cycles",
		planFile: lastDraftFile ? path.basename(lastDraftFile) : undefined,
		workDir,
		totalUsage,
	};
}

// ── Active run state (mirrors ship.ts pattern) ───────────────────

let activePlanRun: { cancelled: boolean } | null = null;

// ── Command Registration ──────────────────────────────────────────

export function registerPlan(pi: ExtensionAPI, currentDepth: number) {
	pi.registerCommand("plan", {
		description: "Run autonomous planning loop: draft→review→fix until convergence (e.g. /plan @objective.md, /plan 42, /plan \"build X\")",
		handler: async (args, ctx) => {
			const spec = args.trim();

			// Handle /plan stop
			if (spec === "stop") {
				if (activePlanRun) {
					activePlanRun.cancelled = true;
					if (ctx.hasUI) ctx.ui.notify("Plan run cancelled.", "warning");
				} else {
					if (ctx.hasUI) ctx.ui.notify("No active plan run to stop.", "info");
				}
				return;
			}

			if (!spec) {
				if (ctx.hasUI) ctx.ui.notify("Usage: /plan <objective | @file | issue#>", "warning");
				return;
			}

			if (activePlanRun && !activePlanRun.cancelled) {
				if (ctx.hasUI) ctx.ui.notify("A plan run is already active. Use /plan stop first.", "error");
				return;
			}

			// Resolve the objective text
			let rawObjective: string;

			if (spec.startsWith("@")) {
				// File reference: /plan @objective.md
				const filePath = spec.slice(1).trim();
				const resolved = path.isAbsolute(filePath) ? filePath : path.join(ctx.cwd, filePath);
				if (!fs.existsSync(resolved)) {
					if (ctx.hasUI) ctx.ui.notify(`File not found: ${resolved}`, "error");
					return;
				}
				rawObjective = fs.readFileSync(resolved, "utf-8");
			} else if (/^#?\d+$/.test(spec)) {
				// Issue number: /plan 42 or /plan #42
				const issueNum = spec.replace(/^#/, "");
				const prompt = [
					`Fetch issue #${issueNum} from the project's forge.`,
					"Run `git remote get-url origin` to detect the forge.",
					"Use `gh` for github.com, `fj` for Forgejo/Gitea, or `curl` API as fallback.",
					"Output ONLY the issue title and body as markdown. Nothing else.",
				].join("\n");

				try {
					const result = await runAgent({
						persona: "You are a utility agent. Follow instructions exactly. Output only what is asked.",
						task: prompt,
						model: utilityModel(),
						tools: ["read", "bash"],
						cwd: ctx.cwd,
						forgeDepth: currentDepth,
					});
					rawObjective = getFinalOutput(result.messages);
					if (!rawObjective.trim()) {
						if (ctx.hasUI) ctx.ui.notify(`Could not fetch issue #${issueNum}`, "error");
						return;
					}
				} catch (err) {
					if (ctx.hasUI) ctx.ui.notify(`Failed to fetch issue #${issueNum}: ${err}`, "error");
					return;
				}
			} else {
				// Inline string: /plan "build the thing"
				rawObjective = spec;
			}

			const { config, objective } = parseObjective(rawObjective);
			const run = { cancelled: false };
			activePlanRun = run;

			const startTime = Date.now();

			try {
				const result = await runPlanLoop(
					objective,
					config,
					ctx.cwd,
					currentDepth,
					undefined, // no AbortSignal from command context
					(msg) => {
						if (ctx.hasUI) ctx.ui.notify(msg, "info");
					},
					() => run.cancelled,
				);

				const duration = Date.now() - startTime;
				logOperation(result.workDir, {
					tool: "plan",
					timestamp: new Date().toISOString(),
					params: { objective: objective.slice(0, 200), config },
					result: result.converged
						? `Converged after ${result.cycles} cycle(s)`
						: `Did NOT converge after ${result.cycles} cycles (${result.reason})`,
					cost: result.totalUsage.cost,
					duration,
				});

				const costLine = result.totalUsage.cost > 0
					? `\nTotal cost: $${result.totalUsage.cost.toFixed(4)}`
					: "";

				if (result.converged) {
					pi.sendUserMessage(
						`Plan converged after ${result.cycles} cycle(s). ` +
						`Read the plan at \`${result.workDir}/PLAN.md\`. ` +
						`Consensus at \`${result.workDir}/plan-review/consensus.md\`.${costLine}`,
					);
				} else {
					pi.sendUserMessage(
						`Plan did NOT converge after ${result.cycles} cycles (${result.reason}). ` +
						`Review drafts and feedback in \`${result.workDir}/\`.${costLine}`,
					);
				}
			} catch (err) {
				if (ctx.hasUI) ctx.ui.notify(`Plan failed: ${err}`, "error");
			} finally {
				if (activePlanRun === run) activePlanRun = null;
			}
		},
	});
}
