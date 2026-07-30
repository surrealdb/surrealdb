// Surrealism module surface over the SDK wire path: `DEFINE MODULE` loads a
// `.surli` archive from a filesystem bucket and publishes its exports as
// callable `mod::<module>::<export>` functions, with the module recorded in the
// database catalog. What this file pins beyond the HTTP-level Rust suite is the
// CBOR/SDK mapping: guest return values arrive as JS values, and every failing
// call arrives as a rejected promise whose message names its cause.
//
// Fixture: the demo archive produced by `surreal module build` from
// `surrealism/demo`. Building it needs a Rust `wasm32-wasip2` toolchain, which
// this suite does not require, so the archive is supplied out of band through
// SURREAL_DEMO_SURLI. A local run without it skips the module tests; a CI run
// without it fails, because there the fixture is always packed.
//
// Expected exports mirror `surrealism/demo/src/lib.rs`: top-level `can_drive`
// and `safe_divide`, `other` (the `name` override on `can_drive_bla`), the
// unnamed default export, the `math` mod (its own default plus `add` and
// `multiply`), and `util` (the `name` override on the `utility_helpers` mod,
// carrying `negate` and the nested `nested::deep`).
//
// Loading a module needs the `files` and `surrealism` experimental capabilities,
// and the bucket directory must be on SURREAL_BUCKET_FOLDER_ALLOWLIST. The
// module declares `allow_net = ["127.0.0.1"]`, so the server must grant at least
// that. Harness trap (README): variadic capability flags take the `--flag=value`
// form, or clap's greedy parser swallows the trailing `memory` positional.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { copyFileSync, existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Surreal } from "surrealdb";
import { rootClient, startServer, type TestServer } from "../src/harness";

const SURLI = process.env.SURREAL_DEMO_SURLI;

if (!SURLI) {
	console.log(
		"surrealism.test.ts: SURREAL_DEMO_SURLI is not set — skipping. Build the fixture with `surreal module build -o <path>/demo.surli surrealism/demo` and point SURREAL_DEMO_SURLI at it.",
	);
}

/** Runs the body only when the `.surli` fixture is available. */
const moduleTest = test.skipIf(!SURLI);

let server: TestServer | undefined;
let bucketDir: string | undefined;

beforeAll(async () => {
	if (!SURLI) return;
	if (!existsSync(SURLI)) {
		throw new Error(`SURREAL_DEMO_SURLI points at a missing file: ${SURLI}`);
	}
	// realpath so the allowlist entry matches the path the server canonicalises
	// (on macOS the temp dir sits behind the /var -> /private/var symlink).
	bucketDir = realpathSync(mkdtempSync(join(tmpdir(), "surrealism-bucket-")));
	copyFileSync(SURLI, join(bucketDir, "demo.surli"));
	server = await startServer({
		args: ["--allow-experimental=files,surrealism", "--allow-net=127.0.0.1"],
		env: { SURREAL_BUCKET_FOLDER_ALLOWLIST: bucketDir },
	});
});

afterAll(async () => {
	await server?.stop();
	if (bucketDir) rmSync(bucketDir, { recursive: true, force: true });
});

/** A root client on a fresh ns/db with the demo module already defined. */
async function moduleClient(): Promise<Surreal> {
	const { db } = await rootClient(server as TestServer);
	const responses = await db
		.query(
			`DEFINE BUCKET test BACKEND "file:${bucketDir}";
			 DEFINE MODULE mod::demo AS f"test:/demo.surli";`,
		)
		.responses();
	for (const r of responses) expect(r.success).toBe(true);
	return db;
}

async function rejects(p: Promise<unknown>): Promise<Error> {
	try {
		await p;
	} catch (e) {
		return e as Error;
	}
	throw new Error("expected promise to reject, but it resolved");
}

/** Evaluate one expression and return its mapped value. */
async function call<T>(db: Surreal, expr: string): Promise<T> {
	const [v] = await db.query<[T]>(`RETURN ${expr}`).json();
	return v;
}

/** Evaluate one expression expected to fail, returning the error message. */
async function failure(db: Surreal, expr: string): Promise<string> {
	return (await rejects(call(db, expr))).message;
}

// Unconditional: the module tests below skip themselves when the fixture is
// absent, which would otherwise let a CI run that lost the packing step report
// green while covering nothing.
test("the packed demo module is supplied under CI", () => {
	if (!process.env.CI) return;
	if (!SURLI) {
		throw new Error(
			"SURREAL_DEMO_SURLI is unset: CI must pack surrealism/demo and point this at the archive, otherwise every module test in this file silently skips.",
		);
	}
	expect(existsSync(SURLI)).toBe(true);
});

moduleTest(
	"module exports are callable as mod:: functions and return typed values",
	async () => {
		const db = await moduleClient();

		// Top-level export, both branches of its predicate.
		expect(await call<boolean>(db, "mod::demo::can_drive(21)")).toBe(true);
		expect(await call<boolean>(db, "mod::demo::can_drive(15)")).toBe(false);

		// The unnamed default export is invoked as the module itself.
		expect(await call<boolean>(db, "mod::demo(21)")).toBe(true);
		expect(await call<boolean>(db, "mod::demo(15)")).toBe(false);

		// A mod namespace: its default export, a named export, and a nested mod.
		expect(await call<number>(db, "mod::demo::math(5)")).toBe(10);
		expect(await call<number>(db, "mod::demo::math::add(3, 4)")).toBe(7);
		expect(await call<number>(db, "mod::demo::math::add(-10, 3)")).toBe(-7);
		expect(await call<number>(db, "mod::demo::util::nested::deep(1)")).toBe(101);

		// A `name` override is the only reachable name, at both function and mod
		// level: `can_drive_bla` -> `other`, `mul` -> `multiply`,
		// `utility_helpers` -> `util`, `neg` -> `negate`.
		expect(await call<boolean>(db, "mod::demo::other(21)")).toBe(true);
		expect(await call<boolean>(db, "mod::demo::other(15)")).toBe(false);
		expect(await call<number>(db, "mod::demo::math::multiply(3, 4)")).toBe(12);
		expect(await call<number>(db, "mod::demo::util::negate(5)")).toBe(-5);

		// A function returning Result maps its Ok branch to a plain value.
		expect(await call<number>(db, "mod::demo::safe_divide(10, 2)")).toBe(5);
		expect(await call<number>(db, "mod::demo::parse_number('42')")).toBe(42);

		await db.close();
	},
	30000,
);

moduleTest(
	"a failing module call rejects with an error naming its cause",
	async () => {
		const db = await moduleClient();

		// A renamed export is not reachable under its Rust name, at either level.
		expect(await failure(db, "mod::demo::can_drive_bla(21)")).toContain("can_drive_bla");
		expect(await failure(db, "mod::demo::math::mul(3, 4)")).toContain("math::mul");
		expect(await failure(db, "mod::demo::utility_helpers::negate(5)")).toContain(
			"utility_helpers::negate",
		);

		// Neither is a name the module never exported, at any nesting depth.
		expect(await failure(db, "mod::demo::nonexistent_function(1)")).toContain(
			"nonexistent_function",
		);
		expect(await failure(db, "mod::demo::math::nonexistent(1)")).toContain("math::nonexistent");
		expect(await failure(db, "mod::demo::util::nested::nonexistent(1)")).toContain(
			"util::nested::nonexistent",
		);

		// A module that was never defined is an error, not a silent NONE.
		expect(await failure(db, "mod::missing::can_drive(21)")).toContain("mod::missing");

		// Arity is checked against the export manifest, before the guest runs.
		const tooFew = await failure(db, "mod::demo::math::add(1)");
		expect(tooFew).toContain("mod::demo::math::add");
		expect(tooFew).toMatch(/expects 2 arguments, but 1/);
		expect(await failure(db, "mod::demo::math::add(1, 2, 3)")).toMatch(
			/expects 2 arguments, but 3/,
		);
		expect(await failure(db, "mod::demo::can_drive()")).toMatch(/expects 1 arguments, but 0/);

		// So are argument types: a non-numeric string cannot coerce to `int`.
		const badType = await failure(db, "mod::demo::can_drive('twenty-one')");
		expect(badType).toMatch(/coerce/i);
		expect(badType).toContain("int");

		// The Err branch of a Result surfaces the guest's own message rather than
		// mapping to NONE.
		expect(await failure(db, "mod::demo::safe_divide(10, 0)")).toContain("Division by zero");
		expect(await failure(db, "mod::demo::parse_number('not_a_number')")).toContain(
			"invalid digit",
		);

		await db.close();
	},
	30000,
);

moduleTest(
	"a defined module appears in the database catalog with its export signatures",
	async () => {
		const db = await moduleClient();

		// INFO FOR DB keys modules by their storage name.
		const [info] = await db
			.query<[{ modules: Record<string, string> }]>("INFO FOR DB")
			.json();
		expect(Object.keys(info.modules)).toContain("mod::demo");
		expect(info.modules["mod::demo"]).toContain("DEFINE MODULE");

		// The STRUCTURE form carries the extracted export manifest, including the
		// writeable flag the host uses to decide whether a call may mutate data.
		interface Export {
			name?: string;
			args: unknown;
			returns: unknown;
			writeable: boolean;
		}
		const [structure] = await db
			.query<[{ modules: Array<{ name: string; exports: Export[] }> }]>(
				"INFO FOR DB STRUCTURE",
			)
			.json();
		const demo = structure.modules.find((m) => m.name === "demo");
		expect(demo).toBeDefined();

		const exports = (demo as { exports: Export[] }).exports;
		const canDrive = exports.find((e) => e.name === "can_drive");
		expect(canDrive).toBeDefined();
		expect((canDrive as Export).writeable).toBe(false);

		// `#[surrealism(writeable)]` is reported, so a read-only module call and a
		// mutating one are distinguishable from the catalog alone.
		const createUser = exports.find((e) => e.name === "create_user");
		expect(createUser).toBeDefined();
		expect((createUser as Export).writeable).toBe(true);

		// The default export is the entry with no name.
		expect(exports.some((e) => e.name === undefined)).toBe(true);

		await db.close();
	},
	30000,
);
