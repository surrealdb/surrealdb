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
	// Resolved against this script rather than the process CWD, so the lookup
	// does not depend on where it was invoked from.
	const manifest = await Bun.file(`${import.meta.dir}/../../Cargo.toml`).text();
	const section = manifest.split(/^\[workspace\.package\]$/m)[1];
	const version = section?.match(/^version\s*=\s*"([^"]+)"/m)?.[1];

	if (!version) {
		console.error("❌ Could not read [workspace.package] version from ../../Cargo.toml");
		process.exit(1);
	}

	return version;
}

/**
 * Is this exact version already on the npm registry? Queries the public registry
 * (no auth needed). A 404 on the package means it does not exist yet (a first
 * publish); any other non-OK status — and any unreadable response body — is
 * treated as "unknown" so the publish still proceeds and npm's own write-once
 * guard has the final say. Never throws: the caller uses it to decide whether to
 * skip or to forgive a publish, and must not lose that publish's exit code to a
 * registry hiccup.
 */
async function isPublished(pkgName: string, pkgVersion: string): Promise<boolean> {
	const url = `https://registry.npmjs.org/${pkgName.replace("/", "%2F")}`;
	let res: Response;
	try {
		res = await fetch(url, { headers: { accept: "application/vnd.npm.install-v1+json" } });
	} catch (err) {
		console.warn(`⚠️ Could not reach the npm registry to check ${pkgName}@${pkgVersion} (${err}); proceeding with publish.`);
		return false;
	}
	if (res.status === 404) return false;
	if (!res.ok) {
		console.warn(`⚠️ Registry check for ${pkgName} returned HTTP ${res.status}; proceeding with publish.`);
		return false;
	}
	try {
		const doc = (await res.json()) as { versions?: Record<string, unknown> };
		return Boolean(doc.versions?.[pkgVersion]);
	} catch (err) {
		console.warn(`⚠️ Could not read the registry response for ${pkgName} (${err}); proceeding with publish.`);
		return false;
	}
}

/**
 * Warn when `latest` does not point where this channel's releases should leave
 * it.
 *
 * npm points `latest` at the FIRST version a package ever publishes whatever
 * `--tag` asked for, so a package whose first release was a prerelease leaves
 * `npm install <pkg>` fetching that prerelease forever — including after newer
 * ones ship under `beta`. Nothing here can move the tag (trusted publishing
 * mints a token for the publish alone), so this reports it for a human to
 * correct with `npm dist-tag`.
 */
async function warnOnMisplacedLatest(name: string): Promise<void> {
	try {
		const res = await fetch(`https://registry.npmjs.org/${name.replace("/", "%2F")}`, {
			headers: { accept: "application/vnd.npm.install-v1+json" },
		});
		if (!res.ok) return;
		const doc = (await res.json()) as { "dist-tags"?: Record<string, string> };
		const latest = doc["dist-tags"]?.latest;
		if (latest && /-/.test(latest)) {
			console.warn(
				`⚠️ ${name}'s \`latest\` tag points at the prerelease ${latest}, so ` +
					`\`npm install ${name}\` resolves to it. Point \`latest\` at a stable ` +
					`release: npm dist-tag add ${name}@<stable> latest`,
			);
		}
	} catch {
		// A registry hiccup must not fail a publish that has already succeeded.
	}
}

const version = process.env.SURREAL_VERSION || (await workspaceVersion());
const pkg = await packageFile.json();
const { name } = pkg;

// A prerelease publishes under a tag named after its label (`3.1.0-beta.4` ->
// `beta`), so only a stable version can ever move `latest`.
const channel = values.channel ?? version.match(/-([0-9A-Za-z]+)/)?.[1] ?? "latest";

console.log(`✨ Publishing ${name} as version ${version} (tag ${channel})`);

// Idempotent skip: if this exact version is already on the registry, do nothing.
// Mirrors crate publishing so "re-run failed jobs" and overwrite re-releases
// converge instead of failing on npm's write-once versions. A dry run still runs
// below — it validates packing without publishing.
if (!values["dry-run"] && (await isPublished(name, version))) {
	console.log(`✅ ${name}@${version} is already published — skipping.`);
	process.exit(0);
}

pkg.version = version;
await Bun.write(packageFile, `${JSON.stringify(pkg, null, "\t")}\n`);

// Packing. The tarball name is the one `bun pm pack` writes: scope marker
// dropped, separator flattened. It has to be passed to npm exactly, because a
// name npm cannot open is read as a flag and it silently packs the directory
// instead.
const safeName = name.replace(/^@/, "").replaceAll("/", "-");

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

// A concurrent or prior publish may have landed this version between the check
// above and now; if the exact version is on the registry, treat it as success —
// the same way crate publishing tolerates an already-uploaded crate. Not for a
// dry run, which never publishes.
if (publishCode !== 0 && !values["dry-run"] && (await isPublished(name, version))) {
	console.log(`✅ ${name}@${version} is already on the registry — treating publish as success.`);
	process.exit(0);
}

if (publishCode === 0 && !values["dry-run"]) {
	await warnOnMisplacedLatest(name);
}

process.exit(publishCode);
