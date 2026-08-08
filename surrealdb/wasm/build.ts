/**
 * Build the `@surrealdb/wasm-native` package.
 *
 * This package is the WebAssembly module and nothing else: the engine that
 * implements the SDK's interface lives in the surrealdb.js repository and
 * depends on this. So there is no TypeScript of ours to bundle — wasm-bindgen
 * already emits the loader and its typings, and they are what gets published.
 *
 * `--debug` builds the unstripped, unwinding profile with the crate's `debug`
 * feature, which reports panics to the browser console. It also skips
 * `wasm-opt`, whose renaming would undo the symbol names that build is for.
 *
 * Requires two Cargo-installed tools: `wasm-bindgen-cli`, at exactly the
 * version the workspace locks (checked below), and `wasm-opt`.
 */

import { rm } from "node:fs/promises";

const [, , ...flags] = Bun.argv;
const isDebug = flags.includes("--debug") || !!process.env.DEBUG_WASM;

const profile = isDebug ? "wasm-release-debug" : "wasm-release";
const features = isDebug ? "kv-indxdb,kv-mem,debug" : "kv-indxdb,kv-mem";

/** The workspace target directory, since this crate is a workspace member. */
const wasmModule = `../../target/wasm32-unknown-unknown/${profile}/surrealdb_wasm.wasm`;

/**
 * Run a build step, failing the build if it fails.
 *
 * Without this a broken `cargo build` leaves the previous `dist/` in place and
 * the package looks like it built.
 */
async function run(step: string, cmd: string[]): Promise<void> {
	console.log(`🔨 ${step}`);

	const code = await Bun.spawn(cmd, {
		stdout: "inherit",
		stderr: "inherit",
	}).exited;

	if (code !== 0) {
		console.error(`❌ ${step} failed (exit ${code})`);
		process.exit(code);
	}
}

/**
 * The wasm-bindgen version the workspace resolves to.
 *
 * The CLI and the crate share a bindgen schema that changes between patch
 * releases, so they have to match exactly. Reading the lockfile rather than
 * pinning a number here means a dependency bump cannot leave this stale — it
 * just asks for the new CLI.
 */
async function lockedBindgenVersion(): Promise<string> {
	const lock = await Bun.file("../../Cargo.lock").text();
	const version = lock.match(/^name = "wasm-bindgen"\nversion = "([^"]+)"/m)?.[1];

	if (!version) {
		console.error("❌ Could not read the wasm-bindgen version from ../../Cargo.lock");
		process.exit(1);
	}

	return version;
}

/** The version of the `wasm-bindgen` on `PATH`, or null if there is none. */
async function installedBindgenVersion(): Promise<string | null> {
	try {
		const proc = Bun.spawn(["wasm-bindgen", "--version"], { stdout: "pipe", stderr: "ignore" });
		const output = await new Response(proc.stdout).text();
		return (await proc.exited) === 0 ? (output.trim().split(/\s+/).at(-1) ?? null) : null;
	} catch {
		return null;
	}
}

const expectedBindgen = await lockedBindgenVersion();
const actualBindgen = await installedBindgenVersion();

if (actualBindgen !== expectedBindgen) {
	console.error(
		actualBindgen
			? `❌ wasm-bindgen ${actualBindgen} is installed, but the workspace locks ${expectedBindgen}`
			: "❌ wasm-bindgen is not installed",
	);
	console.error(`   cargo install -f wasm-bindgen-cli --version ${expectedBindgen}`);
	process.exit(1);
}

// A stale module from an earlier build must not survive a failed one.
await rm("dist", { recursive: true, force: true });

await run(`Compiling the engine (profile: ${profile})`, [
	"cargo",
	"build",
	"--package",
	"surrealdb-wasm",
	"--target",
	"wasm32-unknown-unknown",
	"--profile",
	profile,
	"--features",
	features,
]);

// `web` rather than `bundler`: the loader then works from a plain `<script
// type="module">` as well as through a bundler, and leaves the consumer free to
// supply the module bytes itself.
await run("Generating the bindings", [
	"wasm-bindgen",
	"--target",
	"web",
	"--out-dir",
	"dist",
	"--out-name",
	"index",
	wasmModule,
]);

if (isDebug) {
	console.log("⏭️  Skipping wasm-opt (debug build keeps its symbol names)");
} else {
	await run("Optimizing the module", [
		"wasm-opt",
		"-Oz",
		"--enable-bulk-memory",
		"--enable-nontrapping-float-to-int",
		"dist/index_bg.wasm",
		"-o",
		"dist/index_bg.wasm",
	]);
}

const { size } = await Bun.file("dist/index_bg.wasm").stat();
console.log(`📦 dist/index_bg.wasm — ${(size / 1024 / 1024).toFixed(2)} MiB`);
