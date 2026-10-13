// Database export / import over the WebSocket connection: hostile-identifier
// escaping (a smuggled DEFINE USER must not create a root user), a byte-identical
// export/import round-trip, and table-filtered export.
//
// Per the suite's hard rule, every test spawns its own server and stops it.
import { expect, test } from "bun:test";
import { rootClient, startServer, type TestServer } from "../src/harness";

async function withServer<T>(fn: (server: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer();
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

test("export escapes hostile identifiers, a smuggled DEFINE USER never creates a root user, and the round-trip is byte-identical", async () => {
	await withServer(async (server) => {
		// Building blocks so the (backtick- and backslash-laden) identifiers are
		// unambiguous: BT is a literal backtick, BS a single backslash. This
		// reproduces a hostile export query
		// byte-for-byte, including a DEFINE USER ... ON ROOT smuggled inside a
		// table name (a SurrealQL injection attempt via an unescaped export).
		const BT = "`";
		const BS = "\\";
		const hostile = [
			`define table if not exists ${BT}pwnme666${BS}${BT};${BS}ncreate cats666 set aaaaaa=1;--${BT};`,
			`create ${BT}pwnme667${BS}${BT};${BS}ncreate meow666 set aaaaaa=1;--${BT} set name="hello1";`,
			`relate person:${BT}a${BT}->${BT}friends${BS}${BT};${BS}ncreate meow set meow='yaaay';--${BT}->person:b set meow='meow';`,
			``,
			`define table bla;`,
			`DEFINE FIELD ${BT} ads${BS}${BS}${BS}${BS}${BT} ON TABLE bla TYPE number;`,
			`DEFINE FIELD ${BT} on table bla type number; define table hax -- ${BT} ON TABLE bla TYPE number;`,
			``,
			`relate person:${BT}a${BT}->${BT}friends2${BS}${BT};${BS}nDEFINE USER IF NOT EXISTS pwned ON ROOT PASSWORD 'pwned' ROLES OWNER DURATION FOR SESSION 12h, FOR TOKEN 1m;--${BT}->person:b set meow='meow';`,
		].join("\n");

		const src = await rootClient(server);

		// Every statement parses and runs: the hostile text is treated as
		// (escaped) identifiers, not executable SurrealQL.
		const ran = await src.db.query(hostile).responses();
		expect(ran).toHaveLength(7);
		for (const r of ran) expect(r.success).toBe(true);

		// SECURITY: the smuggled `DEFINE USER pwned ON ROOT` did NOT execute —
		// the only root user is the seeded `root`.
		const [rootBefore] = (await src.db.query("INFO FOR ROOT").json()) as [
			{ users: Record<string, string> },
		];
		expect(Object.keys(rootBefore.users)).not.toContain("pwned");
		expect(Object.keys(rootBefore.users)).toEqual(["root"]);

		// Export the source database over HTTP (exact bytes).
		const auth = `Basic ${btoa("root:root")}`;
		const exportDb = (ns: string, database: string) =>
			fetch(`${server.httpUrl}/export`, {
				headers: { Authorization: auth, "surreal-ns": ns, "surreal-db": database },
			}).then((r) => {
				expect(r.status).toBe(200);
				return r.text();
			});
		const exported = await exportDb(src.namespace, src.database);

		// The DEFINE USER survives in the export ONLY as a backtick-escaped
		// table name — never as a standalone, re-executable statement.
		expect(exported).toContain("OPTION IMPORT;");
		expect(exported).toContain(
			`DEFINE TABLE ${BT}friends2${BS}${BT};${BS}nDEFINE USER IF NOT EXISTS pwned ON ROOT`,
		);

		// Import into a fresh database on the same server, then re-export.
		const dst = await rootClient(server);
		const imp = await fetch(`${server.httpUrl}/import`, {
			method: "POST",
			headers: { Authorization: auth, "surreal-ns": dst.namespace, "surreal-db": dst.database },
			body: exported,
		});
		expect(imp.status).toBe(200);
		const reExported = await exportDb(dst.namespace, dst.database);

		// The escaping is stable: a round-trip reproduces the export byte-for-byte.
		expect(reExported).toBe(exported);

		// SECURITY: importing the export did not execute the injection either.
		const [rootAfter] = (await dst.db.query("INFO FOR ROOT").json()) as [
			{ users: Record<string, string> },
		];
		expect(Object.keys(rootAfter.users)).toEqual(["root"]);

		await src.db.close();
		await dst.db.close();
	});
});

test("SDK export() / import() round-trips a database over the WebSocket connection", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);

		for (let i = 0; i < 10; i++) {
			await db.query("CREATE user SET name = $name", { name: `User ${i}` }).collect();
		}

		const exported = await db.export();
		expect(typeof exported).toBe("string");
		expect(exported).toContain("OPTION IMPORT;");
		expect(exported).toMatch(/DEFINE TABLE user\b/);

		// Drop the table, then import the export back in. (No emptiness check in
		// between: a SELECT from a removed table is a loud NotFoundError,
		// not an empty result — see tests/surrealql-wire.test.ts.)
		await db.query("REMOVE TABLE user").collect();
		await db.import(exported);

		// Every record is present post-import.
		const [names] = (await db.query("SELECT VALUE name FROM user ORDER BY name").json()) as [
			string[],
		];
		expect(names).toEqual([
			"User 0",
			"User 1",
			"User 2",
			"User 3",
			"User 4",
			"User 5",
			"User 6",
			"User 7",
			"User 8",
			"User 9",
		]);

		await db.close();
	});
});

test("export({ tables: [...] }) includes only the requested tables", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		for (let i = 0; i < 5; i++) {
			await db.query("CREATE user SET name = $n", { n: `User ${i}` }).collect();
			await db.query("CREATE grp SET name = $n", { n: `Group ${i}` }).collect();
		}

		const onlyUsers = await db.export({ tables: ["user"] });
		expect(onlyUsers).toContain("DEFINE TABLE user");
		expect(onlyUsers).toMatch(/name: 'User 0'/);
		// The `grp` table is entirely absent from a user-only export.
		expect(onlyUsers).not.toContain("grp");
		expect(onlyUsers).not.toContain("Group 0");

		// Round-trip the selective export into a fresh database: users present,
		// group data never imported. Pre-DEFINE both tables in the destination
		// so the empty-group check is a clean [] and
		// not the NotFoundError that a truly-undefined table would raise.
		const dst = await rootClient(server);
		await dst.db.query("DEFINE TABLE user; DEFINE TABLE grp;").collect();
		await dst.db.import(onlyUsers);
		const [users] = (await dst.db.query("SELECT VALUE name FROM user ORDER BY name").json()) as [
			string[],
		];
		expect(users).toEqual(["User 0", "User 1", "User 2", "User 3", "User 4"]);
		// No group records came across in a user-only export.
		const [groups] = (await dst.db.query("SELECT id FROM grp").json()) as [unknown[]];
		expect(groups).toEqual([]);

		await db.close();
		await dst.db.close();
	});
});
