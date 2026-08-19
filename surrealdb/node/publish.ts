/**
 * Publish `@surrealdb/node-native` to npm.
 *
 * The package version is not committed: see `version.ts` for where it comes
 * from and why the build agrees with it.
 *
 * # One package per platform
 *
 * The addon is around 45 MB per platform and there are eight of them, so the
 * native binaries are NOT published inside `@surrealdb/node-native`. Each is
 * published as its own `@surrealdb/node-native-<platform>` package carrying
 * `os`/`cpu`/`libc`, and the root package lists them all under
 * `optionalDependencies`: npm installs only the one whose constraints the host
 * satisfies, and the loader NAPI generated in `dist/index.js` requires exactly
 * that package. An installer therefore downloads one platform's binary rather
 * than all eight — the difference between roughly 45 MB and 340 MB, which is
 * also the difference between fitting an AWS Lambda deployment and not.
 *
 * The platform packages are published FIRST, so the root package never exists
 * on the registry naming a dependency that does not.
 *
 * Expects `dist/` to already hold the loader and a binary for every target in
 * `napi.targets` (`bun run build.ts` produces one platform's worth, and the
 * release workflow assembles all of them before calling this).
 */

import { rm } from "node:fs/promises";
import { parseArgs } from "node:util";
import { packageVersion } from "./version.ts";

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

/** One staged per-platform package: its npm name and the directory to publish. */
type PlatformPackage = { name: string; dir: string };

/**
 * Stage one npm package per platform, each holding that platform's binary.
 *
 * `napi create-npm-dirs` writes a complete manifest per target in
 * `napi.targets` — its npm platform name, `os`/`cpu`/`libc`, and the binary it
 * carries. Reading that manifest rather than mapping Rust target triples to npm
 * platform strings here keeps this in step with the loader in `dist/index.js`,
 * which NAPI generated from the same mapping: the two have to name the same
 * packages or the loader looks for something nobody published.
 *
 * Every target is checked before anything is written, and the binaries are
 * copied rather than moved: `dist/` is a build output this does not own — a dry
 * run has to be repeatable, and a run that fails partway must not leave the
 * caller with a build to redo. The root package excludes them by pattern
 * instead of by their absence.
 *
 * A binary missing from `dist/` fails the publish rather than dropping that
 * platform, because the root package that follows would then be published
 * claiming support it cannot deliver.
 */
async function stagePlatformPackages(version: string): Promise<PlatformPackage[]> {
	// A stale directory from an earlier run would be published as-is, binary and
	// all, so the set is always regenerated from the current `napi.targets`.
	await rm("npm", { recursive: true, force: true });

	// The locally installed CLI, not `bunx napi`: bunx fetches a package it
	// cannot resolve, and a release publish must not run a tool downloaded at
	// that moment. A missing one is a failed `bun install`, worth failing on.
	const created = await Bun.spawn(
		["./node_modules/.bin/napi", "create-npm-dirs", "--npm-dir", "npm"],
		{ stdout: "inherit", stderr: "inherit" },
	).exited;

	if (created !== 0) {
		console.error(`❌ Could not create the per-platform package directories (exit ${created})`);
		process.exit(created);
	}

	const platforms = await Array.fromAsync(new Bun.Glob("*/package.json").scan("npm"));
	const staged: (PlatformPackage & { binary: string })[] = [];

	for (const platform of platforms.sort()) {
		// `./` is load-bearing: npm reads a bare `a/b` as a GitHub shorthand
		// rather than a directory to pack.
		const dir = `./npm/${platform.replace(/[/\\]package\.json$/, "")}`;
		const manifest = await Bun.file(`${dir}/package.json`).json();
		staged.push({ name: manifest.name, dir, binary: manifest.main });
	}

	if (staged.length === 0) {
		console.error("❌ No per-platform packages were staged; is `napi.targets` set?");
		process.exit(1);
	}

	// Whole-set check first: a publish that stopped at the first missing binary
	// would report one platform at a time across as many rebuilds.
	const missing = (
		await Promise.all(
			staged.map(async ({ binary }) =>
				(await Bun.file(`dist/${binary}`).exists()) ? null : binary,
			),
		)
	).filter((binary): binary is string => binary !== null);

	if (missing.length > 0) {
		console.error(
			`❌ ${missing.length} of ${staged.length} binaries are missing from dist/, so those ` +
				`platform packages would ship none: ${missing.join(", ")}. Every target in ` +
				"`napi.targets` has to be built before publishing.",
		);
		process.exit(1);
	}

	// And the other direction: a binary no target claims would be excluded from
	// the root package and staged into nothing, dropping that platform from the
	// release with everything still reporting success. `napi.targets` and the
	// build matrix are separate lists, and this is what holds them together.
	const claimed = new Set(staged.map(({ binary }) => binary));
	const unclaimed = (await Array.fromAsync(new Bun.Glob("*.node").scan("dist"))).filter(
		(binary) => !claimed.has(binary),
	);

	if (unclaimed.length > 0) {
		console.error(
			`❌ dist/ holds ${unclaimed.length} binaries that no \`napi.targets\` entry claims, so ` +
				`nothing would publish them: ${unclaimed.join(", ")}. Add their targets to ` +
				"`napi.targets`, or stop building them.",
		);
		process.exit(1);
	}

	for (const { dir, binary, name: platform } of staged) {
		await Bun.write(`${dir}/${binary}`, Bun.file(`dist/${binary}`));
		// Each platform package distributes a compiled artefact of an
		// Apache-2.0 work on its own, so it carries the licence itself rather
		// than only naming it in its manifest.
		await Bun.write(`${dir}/LICENSE`, Bun.file("../../LICENSE"));
		const manifestPath = `${dir}/package.json`;
		const manifest = await Bun.file(manifestPath).json();
		manifest.version = version;
		manifest.files = [binary, "LICENSE"];
		await Bun.write(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
		console.log(`📦 Staged ${platform} with ${binary}`);
	}

	return staged.map(({ name, dir }) => ({ name, dir }));
}

/**
 * Publish one package, tolerating a version that is already on the registry.
 *
 * `target` is a packed tarball or a directory npm packs itself. Returns the
 * exit code, with an already-published version reported as success: a
 * concurrent or prior run may have landed it between the check and the publish,
 * the same way crate publishing tolerates an already-uploaded crate.
 */
async function publishPackage(name: string, version: string, target: string): Promise<number> {
	const cmd = ["npm", "publish", target, "--access", "public", "--tag", channel];

	if (values.provenance) {
		cmd.push("--provenance");
	}

	if (values["dry-run"]) {
		cmd.push("--dry-run");
	}

	console.log(`🚀 Publishing ${name}@${version} to ${channel} in NPM...`);

	const code = await Bun.spawn(cmd, { stdout: "inherit", stderr: "inherit" }).exited;

	if (code !== 0 && !values["dry-run"] && (await isPublished(name, version))) {
		console.log(`✅ ${name}@${version} is already on the registry — treating publish as success.`);
		return 0;
	}

	return code;
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

const version = await packageVersion();
const committedManifest = await packageFile.text();
const pkg = JSON.parse(committedManifest);
const { name } = pkg;

/**
 * Put `package.json` back the way it was committed.
 *
 * The publish stamps the version, the `files` pattern and the platform
 * dependencies into it, none of which are committed, and all of which are only
 * needed until the tarball is packed. Leaving them behind hands the next `git
 * add` a manifest pinning versions that may not exist.
 */
async function restoreManifest(): Promise<void> {
	await Bun.write(packageFile, committedManifest);
}

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

if (values["dry-run"]) {
	console.log("🔍 Preparing dry run release...");
}

const platforms = await stagePlatformPackages(version);

pkg.version = version;
// The binaries are staged as copies, so the root package excludes them by
// pattern rather than by their being gone: it ships the loader and its typings
// and depends on the platform packages for the rest. Set here rather than
// committed, because a tarball packed straight out of a local build has no
// platform packages to fall back on and needs the binary beside the loader —
// which is how `external-sdk-tests` hands a build to the JavaScript SDK.
pkg.files = ["dist", "!dist/*.node"];
// Rebuilt rather than merged: the set of platforms is whatever was just staged,
// so a target dropped from `napi.targets` stops being depended on.
pkg.optionalDependencies = Object.fromEntries(
	platforms.map(({ name: platform }) => [platform, version]),
);
await Bun.write(packageFile, `${JSON.stringify(pkg, null, "\t")}\n`);

// The root package is unusable without the binary its host needs, so nothing
// reaches the registry naming a package that is not there yet.
for (const platform of platforms) {
	if (!values["dry-run"] && (await isPublished(platform.name, version))) {
		console.log(`✅ ${platform.name}@${version} is already published — skipping.`);
		continue;
	}

	const code = await publishPackage(platform.name, version, platform.dir);

	if (code !== 0) {
		console.error(
			`❌ Publishing ${platform.name}@${version} failed; ${name} was left unpublished.`,
		);
		await restoreManifest();
		process.exit(code);
	}
}

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
	await restoreManifest();
	process.exit(packCode);
}

const publishCode = await publishPackage(name, version, `${safeName}-${version}.tgz`);

// Packing is done, so the manifest has served its purpose whether or not the
// publish itself succeeded.
await restoreManifest();

if (publishCode === 0 && !values["dry-run"]) {
	await warnOnMisplacedLatest(name);
}

process.exit(publishCode);
