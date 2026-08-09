import { copyFile, mkdir } from "node:fs/promises";
import { basename } from "node:path";
import { Glob } from "bun";
import dedent from "dedent";

const isWindows = process.platform === "win32";
const [, , ...flags] = Bun.argv;

// Build the NAPI binary
console.log("🔨 Building the NAPI binary");

const DTS_HEADER = dedent`
	type CapabilitiesAllowDenyList = {
		allow?: boolean | string[];
		deny?: boolean | string[];
	};

	type ConnectionOptions = {
		/** Query timeout in whole seconds. */
		query_timeout?: number;
		/** Transaction timeout in whole seconds. */
		transaction_timeout?: number;
		/**
		 * The namespace and database created when the storage is new.
		 * Defaults to \`main\`/\`main\`; pass \`false\` to create neither.
		 */
		defaults?:
			| boolean
			| {
				namespace?: string;
				database?: string;
			};
		capabilities?:
			| boolean
			| {
				scripting?: boolean;
				guest_access?: boolean;
				live_query_notifications?: boolean;
				functions?: boolean | string[] | CapabilitiesAllowDenyList;
				network_targets?: boolean | string[] | CapabilitiesAllowDenyList;
				experimental?: boolean | string[] | CapabilitiesAllowDenyList;
				planner_strategy?: "best-effort" | "compute-only" | "all-read-only";
			};
	};
	\n
`;

const dtsHeader = isWindows ? `"${DTS_HEADER}"` : DTS_HEADER; // This makes me weep
const buildCmd = [
	"bunx",
	"napi",
	"build",
	"-s",
	"--esm",
	"--strip",
	"--dts-header",
	dtsHeader,
	"--platform",
	"--release",
	"--features",
	"kv-rocksdb,kv-mem,kv-surrealkv",
	"-o",
	"napi",
];

if (flags.length > 0) {
	buildCmd.push(...flags);
	console.log(`🎯 NAPI flags: ${flags.join(" ")}`);
}

const code = await Bun.spawn(buildCmd, {
	stdout: "inherit",
	stderr: "inherit",
	env: {
		...process.env,
		CFLAGS_aarch64_unknown_linux_gnu: "-D__ARM_ARCH=8",
		CXX_aarch64_unknown_linux_gnu: "aarch64-linux-gnu-g++",
		CC_aarch64_unknown_linux_gnu: "aarch64-linux-gnu-gcc",
	},
}).exited;

// Without this a broken build leaves the previous `dist/` in place and the
// package looks like it built.
if (code !== 0) {
	console.error(`❌ Building the NAPI binary failed (exit ${code})`);
	process.exit(code);
}

// Assemble the package
//
// This package is the NAPI addon and nothing else: the engine that implements
// the SDK's interface lives in the surrealdb.js repository and depends on this.
// So there is no TypeScript of ours to bundle — NAPI already emits the loader
// and its typings, and they are what gets published.
console.log("📦 Assembling the package");

await mkdir("dist", { recursive: true });
await copyFile("napi/index.js", "dist/index.js");
await copyFile("napi/index.d.ts", "dist/index.d.ts");

for await (const file of new Glob("napi/*.node").scan(".")) {
	await copyFile(file, `dist/${basename(file)}`);
}
