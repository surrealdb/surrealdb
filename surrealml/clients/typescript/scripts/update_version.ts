#!/usr/bin/env node
import { promises as fs } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";


const SEMVER_RE = /^\d+\.\d+\.\d+$/;
const __filename = fileURLToPath(import.meta.url);
const __dirname  = dirname(__filename);
const CONFIG_JSON_PATH = join(__dirname, "..", "config.json");


function validateSemver(v: string, label: string): string {
	if (!SEMVER_RE.test(v)) {
		console.error(`Invalid ${label} '${v}': must be in format X.Y.Z`);
		process.exit(1);
	}
	return v;
}

async function main() {
    const args = process.argv.slice(2);

    // Expect: <npm_package_version> [dynamic_lib_version]
    if (args.length < 1 || args.length > 2 || !args[0]) {
		console.error("Usage: bump-version <npm_package_version> [dynamic_lib_version]");
		process.exit(1);
    }

    const newNpm = validateSemver(args[0], "npm_package_version");
    const newDyn = args[1] ? validateSemver(args[1], "dynamic_lib_version") : undefined;

    // load config.json
    let cfg: Record<string, any>;
    try {
		const raw = await fs.readFile(CONFIG_JSON_PATH, "utf-8");
		cfg = JSON.parse(raw);

    } catch (err: any) {
		console.error(`Error reading '${CONFIG_JSON_PATH}': ${err.message}`);
		process.exit(1);
    }

    // always update npm_package_version
    cfg.npm_package_version = newNpm;

    // optionally update dynamic_lib_version
    if (newDyn) cfg.dynamic_lib_version = newDyn;

    // write back
    try {
		const out = JSON.stringify(cfg, null, 4) + "\n";
		await fs.writeFile(CONFIG_JSON_PATH, out, "utf-8");
    } catch (err: any) {
		console.error(`Error writing '${CONFIG_JSON_PATH}': ${err.message}`);
		process.exit(1);
    }

    console.log("✅  config.json updated");
}

main();
