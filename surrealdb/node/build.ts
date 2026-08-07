import { copyFile } from "node:fs/promises";
import { basename } from "node:path";
import { Glob } from "bun";
import dedent from "dedent";
import { rolldown } from "rolldown";

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
		strict?: boolean;
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

await Bun.spawn(buildCmd, {
    stdout: "inherit",
    stderr: "inherit",
    env: {
        ...process.env,
        CFLAGS_aarch64_unknown_linux_gnu: "-D__ARM_ARCH=8",
        CXX_aarch64_unknown_linux_gnu: "aarch64-linux-gnu-g++",
        CC_aarch64_unknown_linux_gnu: "aarch64-linux-gnu-gcc",
    },
}).exited;

// Bundle the engine implementation
console.log("🔨 Generating the package bundle");

const bundle = await rolldown({
    input: "./src-ts/index.ts",
    // The NAPI loader the CLI generates imports `module` unprefixed.
    external: ["surrealdb", "node:module", "module"],
});

// ESModule only (we require top level await)
await bundle.write({
    format: "esm",
    file: "./dist/surrealdb-node.mjs",
});

// TS Declaration
const task = Bun.spawn(
    [
        "bunx",
        "dts-bundle-generator",
        "--project",
        "tsconfig.types.json",
        "--no-check",
        "--disable-symlinks-following",
        "--export-referenced-types",
        "false",
        "-o",
        "./dist/surrealdb-node.d.ts",
        "./src-ts/index.ts",
    ],
    {
        stdout: "inherit",
        stderr: "inherit",
        async onExit(_, exitCode) {
            if (exitCode !== 0) process.exit(exitCode);
        },
    },
);

await task.exited;

// Copy the NAPI binary
for await (const file of new Glob("napi/*.node").scan(".")) {
    await copyFile(file, `dist/${basename(file)}`);
}
