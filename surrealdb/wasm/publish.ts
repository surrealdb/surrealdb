/**
 * Publish `@surrealdb/wasm-native` to npm.
 *
 * The package version is not committed: it is the engine version, taken from
 * `SURREAL_VERSION` when the release workflow supplies one and otherwise read
 * out of the Cargo workspace. That keeps the module's version identical to the
 * `surrealdb` release whose engine it embeds, with nothing to bump by hand.
 *
 * Expects `dist/` to already hold a build (`bun run build.ts` produces one).
 */

import { parseArgs } from "node:util";

const { values } = parseArgs({
	args: Bun.argv.slice(2),
	options: {
		"dry-run": { type: "boolean", default: false },
		// npm can only attest provenance for a package built in a public
		// repository, so the caller decides whether to ask for it.
		provenance: { type: "boolean", default: false },
		channel: { type: "string" },
	},
});

const packageFile = Bun.file("package.json");

if (!(await packageFile.exists())) {
	console.error("❌ Required package.json not found");
	process.exit(1);
}

/** Read the shared crate version out of the Cargo workspace manifest. */
async function workspaceVersion(): Promise<string> {
	const manifest = await Bun.file("../../Cargo.toml").text();
	const section = manifest.split(/^\[workspace\.package\]$/m)[1];
	const version = section?.match(/^version\s*=\s*"([^"]+)"/m)?.[1];

	if (!version) {
		console.error("❌ Could not read [workspace.package] version from ../../Cargo.toml");
		process.exit(1);
	}

	return version;
}

const version = process.env.SURREAL_VERSION || (await workspaceVersion());
const pkg = await packageFile.json();
const { name } = pkg;

// A prerelease publishes under a tag named after its label (`3.1.0-beta.4` ->
// `beta`), so only a stable version can ever move `latest`.
const channel = values.channel ?? version.match(/-([0-9A-Za-z]+)/)?.[1] ?? "latest";

console.log(`✨ Publishing ${name} as version ${version} (tag ${channel})`);

pkg.version = version;
await Bun.write(packageFile, `${JSON.stringify(pkg, null, "\t")}\n`);

// Packing
const safeName = name.replaceAll("@", "-");

console.log(`📦 Packing ${name}@${version}...`);

const packCode = await Bun.spawn(["bun", "pm", "pack"], {
	stdout: "inherit",
	stderr: "inherit",
}).exited;

if (packCode !== 0) {
	console.error("❌ Pack failed");
	process.exit(packCode);
}

// Publishing
const publishCmd = [
	"npm",
	"publish",
	`${safeName}-${version}.tgz`,
	"--access",
	"public",
	"--tag",
	channel,
];

if (values.provenance) {
	publishCmd.push("--provenance");
}

if (values["dry-run"]) {
	console.log("🔍 Preparing dry run release...");
	publishCmd.push("--dry-run");
}

console.log(`🚀 Publishing ${name}@${version} to ${channel} in NPM...`);

const publishCode = await Bun.spawn(publishCmd, {
	stdout: "inherit",
	stderr: "inherit",
}).exited;

process.exit(publishCode);
