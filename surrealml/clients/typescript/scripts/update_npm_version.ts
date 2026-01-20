#!/usr/bin/env node
import { promises as fs } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";


const SEMVER_RE = /^\d+\.\d+\.\d+$/;
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const CONFIG_JSON_PATH = join(__dirname, "..", "config.json");      // dist/config.json
const PACKAGE_JSON_PATH = join(__dirname, "..", "package.json"); // repo-root/package.json


function assertSemver(v: unknown, label: string): asserts v is string {
    if (typeof v !== "string" || !SEMVER_RE.test(v)) {
      	throw new Error(`Invalid ${label}: expected X.Y.Z, got ${String(v)}`);
    }
}

async function main() {
	// read config.json
	let cfg: Record<string, any>;
	try {
		const raw = await fs.readFile(CONFIG_JSON_PATH, "utf-8");
		cfg = JSON.parse(raw);
	} catch (err: any) {
		console.error(`❌ Cannot read ${CONFIG_JSON_PATH}: ${err.message}`);
		process.exit(1);
	}

	// validate version
	try {
		assertSemver(cfg.npm_package_version, "npm_package_version in config.json");
	} catch (err: any) {
		console.error(`❌ ${err.message}`);
		process.exit(1);
	}
	const targetVersion = cfg.npm_package_version;

	// read package.json
	let pkg: Record<string, any>;
	try {
		const raw = await fs.readFile(PACKAGE_JSON_PATH, "utf-8");
		pkg = JSON.parse(raw);
	} catch (err: any) {
		console.error(`❌ Cannot read ${PACKAGE_JSON_PATH}: ${err.message}`);
		process.exit(1);
	}

	// check if already up-to-date
	if (pkg.version === targetVersion) {
		console.log(`ℹ️  package.json already at ${targetVersion} — nothing to do`);
		return;
	}

	// update & write back
	try {
		pkg.version = targetVersion;
		const out = JSON.stringify(pkg, null, 2) + "\n";
		await fs.writeFile(PACKAGE_JSON_PATH, out, "utf-8");
	} catch (err: any) {
		console.error(`❌ Cannot write ${PACKAGE_JSON_PATH}: ${err.message}`);
		process.exit(1);
	}

	console.log(`✅ package.json version set to ${targetVersion}`);
}

main();
