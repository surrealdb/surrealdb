import { afterAll, beforeAll, expect, test } from "bun:test";
import { Surreal } from "surrealdb";
import { guestClient, rootClient, startServer, type TestServer } from "../src/harness";

// Programmability surface: DEFINE FUNCTION and DEFINE PARAM permission
// enforcement. The PERMISSIONS clause on a custom function or a global param
// gates NON-root callers; root (and other system users) bypass the predicate.
// A record user is allowed only when the predicate is truthy for them, and a
// denial surfaces as a loud error — the same on the `run` RPC path (db.run) as
// on the `query` path (db.query("RETURN ...")). Params defined by root are
// global and visible to any later connection on the same ns/db.
// Every scenario stays within a single namespace/database.

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

async function rejects(p: Promise<unknown>): Promise<Error> {
	try {
		await p;
	} catch (e) {
		return e as Error;
	}
	throw new Error("expected promise to reject, but it resolved");
}

// A record access whose users carry an `admin` flag, so a PERMISSIONS predicate
// can be made true for one record user and false for another.
const RECORD_ACCESS_SETUP = `
	DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email, admin = $admin )
		SIGNIN ( SELECT * FROM user WHERE email = $email )
		DURATION FOR TOKEN 15m, FOR SESSION 12h;
`;

async function recordUser(
	namespace: string,
	database: string,
	email: string,
	admin: boolean,
): Promise<Surreal> {
	const client = await guestClient(server, namespace, database);
	await client.signup({
		namespace,
		database,
		access: "account",
		variables: { email, admin },
	});
	return client;
}

// §3 — DEFINE FUNCTION PERMISSIONS

test(
	"DEFINE FUNCTION PERMISSIONS gate non-root callers identically on the query and run paths",
	async () => {
		const { db, namespace, database } = await rootClient(server);
		await db.query(RECORD_ACCESS_SETUP);
		await db.query(`
			DEFINE FUNCTION fn::pub() { RETURN 1 } PERMISSIONS FULL;
			DEFINE FUNCTION fn::priv() { RETURN 2 } PERMISSIONS NONE;
			DEFINE FUNCTION fn::gated() { RETURN 3 } PERMISSIONS WHERE $auth.admin = true;
		`);

		const boss = await recordUser(namespace, database, "boss@example.com", true);
		const peon = await recordUser(namespace, database, "peon@example.com", false);

		// Invoke a function through both SDK entrypoints, returning either the
		// value or the rejection error.
		const viaQuery = async (client: Surreal, fn: string): Promise<number | Error> => {
			try {
				const [v] = await client.query<[number]>(`RETURN ${fn}()`).json();
				return v;
			} catch (e) {
				return e as Error;
			}
		};
		const viaRun = async (client: Surreal, fn: string): Promise<number | Error> => {
			try {
				return await client.run<number>(fn).json();
			} catch (e) {
				return e as Error;
			}
		};

		// Root bypasses every PERMISSIONS clause on both paths.
		expect(await viaQuery(db, "fn::pub")).toBe(1);
		expect(await viaQuery(db, "fn::priv")).toBe(2);
		expect(await viaQuery(db, "fn::gated")).toBe(3);
		expect(await viaRun(db, "fn::pub")).toBe(1);
		expect(await viaRun(db, "fn::priv")).toBe(2);
		expect(await viaRun(db, "fn::gated")).toBe(3);

		// PERMISSIONS FULL runs for any record user on both paths.
		expect(await viaQuery(boss, "fn::pub")).toBe(1);
		expect(await viaRun(boss, "fn::pub")).toBe(1);
		expect(await viaQuery(peon, "fn::pub")).toBe(1);
		expect(await viaRun(peon, "fn::pub")).toBe(1);

		// PERMISSIONS NONE denies every record user. Denial is a loud error, not a
		// silent NONE; the message names the function, and run and query agree.
		for (const client of [boss, peon]) {
			const q = await viaQuery(client, "fn::priv");
			const r = await viaRun(client, "fn::priv");
			expect(q).toBeInstanceOf(Error);
			expect(r).toBeInstanceOf(Error);
			expect((q as Error).message).toContain(
				"You don't have permission to run the fn::priv function",
			);
			expect((r as Error).message).toContain(
				"You don't have permission to run the fn::priv function",
			);
		}

		// PERMISSIONS WHERE gates per-user: truthy for the admin, falsy for the
		// plain user. The falsy branch is the same loud error on both paths. The
		// predicate dereferences the auth record's `admin` field, and the function
		// path resolves that fetch correctly (contrast the param path, below).
		expect(await viaQuery(boss, "fn::gated")).toBe(3);
		expect(await viaRun(boss, "fn::gated")).toBe(3);
		const gq = await viaQuery(peon, "fn::gated");
		const gr = await viaRun(peon, "fn::gated");
		expect(gq).toBeInstanceOf(Error);
		expect(gr).toBeInstanceOf(Error);
		expect((gq as Error).message).toContain(
			"You don't have permission to run the fn::gated function",
		);
		expect((gr as Error).message).toContain(
			"You don't have permission to run the fn::gated function",
		);

		await boss.close();
		await peon.close();
		await db.close();
	},
	30000,
);

// §4 — DEFINE PARAM PERMISSIONS + cross-connection visibility

async function readParam(client: Surreal, name: string): Promise<unknown | Error> {
	try {
		const [v] = await client.query<[unknown]>(`RETURN ${name}`).json();
		return v;
	} catch (e) {
		return e as Error;
	}
}

test(
	"DEFINE PARAM PERMISSIONS gate non-root readers; root reads every param",
	async () => {
		const { db, namespace, database } = await rootClient(server);
		await db.query(RECORD_ACCESS_SETUP);
		await db.query(`
			DEFINE PARAM $answer VALUE 42 PERMISSIONS FULL;
			DEFINE PARAM $secret VALUE 7 PERMISSIONS NONE;
			DEFINE PARAM $locked VALUE 9 PERMISSIONS WHERE false;
		`);

		const boss = await recordUser(namespace, database, "boss@example.com", true);
		const peon = await recordUser(namespace, database, "peon@example.com", false);

		// Root bypasses the PERMISSIONS clause and reads every param value.
		expect(await readParam(db, "$answer")).toBe(42);
		expect(await readParam(db, "$secret")).toBe(7);
		expect(await readParam(db, "$locked")).toBe(9);

		// PERMISSIONS FULL is readable by any record user.
		expect(await readParam(boss, "$answer")).toBe(42);
		expect(await readParam(peon, "$answer")).toBe(42);

		// PERMISSIONS NONE, and a PERMISSIONS WHERE predicate that evaluates falsy,
		// both deny every record user with a loud, param-named error — never a
		// silent NONE.
		for (const client of [boss, peon]) {
			const none = await readParam(client, "$secret");
			expect(none).toBeInstanceOf(Error);
			expect((none as Error).message).toContain(
				"You don't have permission to view the $secret parameter",
			);
			const locked = await readParam(client, "$locked");
			expect(locked).toBeInstanceOf(Error);
			expect((locked as Error).message).toContain(
				"You don't have permission to view the $locked parameter",
			);
		}

		await boss.close();
		await peon.close();
		await db.close();
	},
	30000,
);

test(
	"a param PERMISSIONS predicate gates a record user by the auth record's fields",
	async () => {
		// A param whose permission predicate dereferences the auth record gates a
		// record user by that record's fields, like the function path: the admin
		// reads the value and the plain user is denied with a param-permission error.
		const { db, namespace, database } = await rootClient(server);
		await db.query(RECORD_ACCESS_SETUP);
		await db.query("DEFINE PARAM $gated VALUE 9 PERMISSIONS WHERE $auth.admin = true");

		const boss = await recordUser(namespace, database, "boss@example.com", true);
		const peon = await recordUser(namespace, database, "peon@example.com", false);

		expect(await readParam(boss, "$gated")).toBe(9);
		const denied = await readParam(peon, "$gated");
		expect(denied).toBeInstanceOf(Error);
		expect((denied as Error).message).toContain(
			"You don't have permission to view the $gated parameter",
		);

		await boss.close();
		await peon.close();
		await db.close();
	},
	30000,
);

test(
	"a global param defined by root on one connection is visible from a fresh connection to the same ns/db",
	async () => {
		const { db, namespace, database } = await rootClient(server);
		await db.query("DEFINE PARAM $shared VALUE 'persisted' PERMISSIONS FULL");

		// A separate SDK connection to the SAME ns/db sees the global param — it is
		// catalog state, not per-session state.
		const other = new Surreal();
		await other.connect(server.url, {
			authentication: { username: "root", password: "root" },
		});
		await other.use({ namespace, database });

		const [seen] = await other.query<[string]>("RETURN $shared").json();
		expect(seen).toBe("persisted");

		// It also shows up in the database catalog listing.
		const [info] = await other
			.query<[{ params: Record<string, string> }]>("INFO FOR DB")
			.json();
		expect(info.params).toHaveProperty("shared");

		await other.close();
		await db.close();
	},
	30000,
);
