/**
 * providers — Detect which API providers have valid credentials.
 *
 * Checks ~/.pi/agent/auth.json (OAuth tokens) and environment variables
 * to determine which providers are available for dispatch/debate.
 * Results are cached for the process lifetime.
 */

import * as fs from "node:fs";
import * as path from "node:path";

/** Maps provider name → { envVar, authKey } for credential detection. */
const PROVIDER_CREDS: Record<string, { envVar: string; authKey: string }> = {
	anthropic: { envVar: "ANTHROPIC_API_KEY", authKey: "anthropic" },
	google: { envVar: "GEMINI_API_KEY", authKey: "google" },
	openai: { envVar: "OPENAI_API_KEY", authKey: "openai" },
	"openai-codex": { envVar: "OPENAI_API_KEY", authKey: "openai-codex" },
};

const AUTH_FILE = path.join(process.env.HOME || "", ".pi", "agent", "auth.json");

let cached: string[] | null = null;

function readAuthFile(): Record<string, unknown> {
	try {
		const raw = fs.readFileSync(AUTH_FILE, "utf-8");
		return JSON.parse(raw) as Record<string, unknown>;
	} catch {
		return {};
	}
}

/**
 * Return list of providers that have valid credentials configured.
 * Checks env vars and ~/.pi/agent/auth.json. Cached for process lifetime.
 * Falls back to ["anthropic"] if nothing is detected (we must be running inside pi).
 */
export function availableProviders(): string[] {
	if (cached) return cached;

	const auth = readAuthFile();
	const available = new Set<string>();

	for (const [provider, { envVar, authKey }] of Object.entries(PROVIDER_CREDS)) {
		if (process.env[envVar]) {
			available.add(provider);
		}
		if (auth[authKey] && typeof auth[authKey] === "object") {
			available.add(provider);
		}
	}

	cached = available.size > 0 ? [...available] : ["anthropic"];
	return cached;
}

/** Check if a specific provider has credentials configured. */
export function hasProvider(name: string): boolean {
	return availableProviders().includes(name);
}
