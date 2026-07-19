// Server-behavior tests ported from the Rust SDK suite
// (surrealdb/tests/api_integration/*). This file ports the SUBSET of those
// tests that exercise the SERVER over the wire rather than Rust SDK types:
//
//   - changefeeds over RPC: DEFINE TABLE ... CHANGEFEED, mutate, then
//     SHOW CHANGES FOR TABLE ... SINCE ... (ported from basic.rs `changefeed`).
//   - export/import: hostile-identifier escaping + byte-identical round-trip
//     (backup.rs `export_escaped_table_names`), plus the SDK export()/import()
//     surface the README lists as uncovered (backup.rs `export_import`,
//     `export_with_config`).
//   - query results over the wire: bindings, ORDER BY/START/LIMIT, record-id
//     ranges, FETCH, DELETE ranges, TYPE decimal coercion, UPDATE CONTENT
//     (basic.rs `query`/`query_binds`, `select_records_order_by*`,
//     `select_record_ranges`, `select_records_fetch`, `delete_record_range`,
//     `query_decimals`, `update_table_with_content`).
//
// NOT ported (they stay Rust): tests asserting Rust SurrealValue typed
// deserialization, the builder API surface, error-kind enums, or embedded
// -engine behavior.
//
// Per the suite's hard rule, every test spawns its OWN server and stops it.
import { expect, test } from "bun:test";
import { RecordId } from "surrealdb";
import { rootClient, startServer, type TestServer } from "../src/harness";

// Each test gets a fresh, isolated server (important for the export test, which
// inspects INFO FOR ROOT for an injected user — a shared server could leak).
// The `finally` guarantees the server is stopped even if an assertion throws.
async function withServer<T>(fn: (server: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer();
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

// ---------------------------------------------------------------------------
// Changefeeds over the wire (ported from basic.rs `changefeed`).
// ---------------------------------------------------------------------------

test("changefeed: SHOW CHANGES SINCE 0 returns the full ordered history with monotonic versionstamps", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);

		// A change feed retains a window of table mutations addressable by a
		// monotonic versionstamp.
		await db.query("DEFINE TABLE testuser CHANGEFEED 1h").collect();
		// Three separate statements => three separate versionstamps. Note the
		// CREATEs surface in the feed as `update` actions, not `create`.
		await db
			.query(
				`CREATE testuser:amos SET name = 'Amos';
				 CREATE testuser:jane SET name = 'Jane';
				 UPDATE testuser:amos SET name = 'AMOS';`,
			)
			.collect();
		// A single table-wide UPDATE CONTENT touches both rows in ONE
		// versionstamp => that feed entry carries two changes.
		await db.query("UPDATE testuser CONTENT { name: 'Doe' }").collect();

		const [entries] = (await db
			.query("SHOW CHANGES FOR TABLE testuser SINCE 0 LIMIT 10")
			.json()) as [
			Array<{ versionstamp: bigint; changes: Array<Record<string, unknown>> }>,
		];

		expect(entries).toHaveLength(5);

		// Entry 0: the DEFINE TABLE itself is recorded in the feed.
		expect(entries[0].changes).toEqual([
			{
				define_table: {
					id: 0,
					name: "testuser",
					changefeed: { expiry: "1h", original: false },
					drop: false,
					kind: { kind: "ANY" },
					permissions: { create: false, delete: false, select: false, update: false },
					schemafull: false,
				},
			},
		]);
		// Entries 1-3: each single-record mutation, in commit order.
		expect(entries[1].changes).toEqual([{ update: { id: "testuser:amos", name: "Amos" } }]);
		expect(entries[2].changes).toEqual([{ update: { id: "testuser:jane", name: "Jane" } }]);
		expect(entries[3].changes).toEqual([{ update: { id: "testuser:amos", name: "AMOS" } }]);
		// Entry 4: the table-wide UPDATE CONTENT — both rows under one entry.
		expect(entries[4].changes).toEqual([
			{ update: { id: "testuser:amos", name: "Doe" } },
			{ update: { id: "testuser:jane", name: "Doe" } },
		]);

		// Versionstamps arrive as JS bigints and increase strictly across the
		// whole feed (the Rust test only pins entries 1..4; full monotonicity
		// holds and is the stronger observed guarantee).
		for (const e of entries) expect(typeof e.versionstamp).toBe("bigint");
		for (let i = 1; i < entries.length; i++) {
			expect(entries[i].versionstamp > entries[i - 1].versionstamp).toBe(true);
		}

		await db.close();
	});
});

test("changefeed: SHOW CHANGES SINCE <versionstamp> is inclusive of the boundary and streams create/update/delete", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);

		await db.query("DEFINE TABLE t CHANGEFEED 1h").collect();
		await db.query("CREATE t:a SET n = 1").collect();

		// Poll the feed and remember the versionstamp of the CREATE.
		const [firstBatch] = (await db.query("SHOW CHANGES FOR TABLE t SINCE 0 LIMIT 10").json()) as [
			Array<{ versionstamp: bigint; changes: Array<Record<string, unknown>> }>,
		];
		// define_table entry + the create-as-update entry.
		expect(firstBatch).toHaveLength(2);
		const createVs = firstBatch[1].versionstamp;
		expect(firstBatch[1].changes).toEqual([{ update: { id: "t:a", n: 1 } }]);

		// Make two more changes, then re-poll SINCE the create's versionstamp.
		await db.query("UPDATE t:a SET n = 2").collect();
		await db.query("DELETE t:a").collect();

		const [batch] = (await db
			.query(`SHOW CHANGES FOR TABLE t SINCE ${createVs} LIMIT 10`)
			.json()) as [
			Array<{ versionstamp: bigint; changes: Array<Record<string, unknown>> }>,
		];

		// SINCE is INCLUSIVE of the supplied versionstamp: the change AT
		// `createVs` is re-delivered, so incremental pollers that pass the
		// last-seen versionstamp must dedup (or poll `SINCE last + 1`). Pinned.
		expect(batch[0].versionstamp).toBe(createVs);
		expect(batch.map((e) => e.changes)).toEqual([
			[{ update: { id: "t:a", n: 1 } }],
			[{ update: { id: "t:a", n: 2 } }],
			[{ delete: { id: "t:a" } }], // DELETE surfaces as a `delete` action
		]);

		await db.close();
	});
});

test("changefeed: SHOW CHANGES on a table without CHANGEFEED returns an empty result, not an error", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);

		await db.query("CREATE plain:a SET n = 1").collect();

		// Querying the change feed of a table that never had one defined is NOT
		// an error — it succeeds with an empty result set.
		const [resp] = await db.query("SHOW CHANGES FOR TABLE plain SINCE 0 LIMIT 10").responses();
		expect(resp.success).toBe(true);
		if (resp.success) expect(resp.result).toEqual([]);

		await db.close();
	});
});

// ---------------------------------------------------------------------------
// Export / import (ported from backup.rs).
// ---------------------------------------------------------------------------

test("export escapes hostile identifiers, a smuggled DEFINE USER never creates a root user, and the round-trip is byte-identical", async () => {
	await withServer(async (server) => {
		// Building blocks so the (backtick- and backslash-laden) identifiers are
		// unambiguous: BT is a literal backtick, BS a single backslash. This
		// reproduces the hostile query from backup.rs `export_escaped_table_names`
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
	// Ported from backup.rs `export_import`; also fills the README gap
	// "Export/import through the SDK (only the raw HTTP surface is covered)".
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
		// between: on 3.x SELECT from a removed table is a loud NotFoundError,
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
	// Ported from backup.rs `export_with_config`.
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
		// (mirroring the Rust test) so the empty-group check is a clean [] and
		// not the 3.x NotFoundError that a truly-undefined table would raise.
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

// ---------------------------------------------------------------------------
// Query results over the wire (ported from basic.rs).
// ---------------------------------------------------------------------------

test("query bindings: CREATE ... SET name = $name is readable back, and a bound record id resolves", async () => {
	// Ported from basic.rs `query` / `query_raw` / `query_binds`.
	await withServer(async (server) => {
		const { db } = await rootClient(server);

		const [created] = (await db
			.query("CREATE user:john SET name = $name", { name: "John Doe" })
			.json()) as [Array<{ id: string; name: string }>];
		expect(created).toEqual([{ id: "user:john", name: "John Doe" }]);

		const [selected] = (await db.query("SELECT name FROM user:john").json()) as [
			Array<{ name: string }>,
		];
		expect(selected).toEqual([{ name: "John Doe" }]);

		// A record id supplied as a binding resolves as the FROM target.
		const [byRid] = (await db
			.query("SELECT * FROM $record_id", { record_id: new RecordId("user", "john") })
			.json()) as [Array<{ id: string; name: string }>];
		expect(byRid).toEqual([{ id: "user:john", name: "John Doe" }]);

		await db.close();
	});
});

test("SELECT ... ORDER BY DESC honors START and LIMIT", async () => {
	// Ported from basic.rs `select_records_order_by_start_limit` / `_order_by`.
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE user:john SET name = 'John';
				 CREATE user:zoey SET name = 'Zoey';
				 CREATE user:amos SET name = 'Amos';
				 CREATE user:jane SET name = 'Jane';`,
			)
			.collect();

		const names = async (q: string) => {
			const [rows] = (await db.query(q).json()) as [Array<{ name: string }>];
			return rows.map((r) => r.name);
		};

		expect(await names("SELECT name FROM user ORDER BY name DESC")).toEqual([
			"Zoey",
			"John",
			"Jane",
			"Amos",
		]);
		expect(await names("SELECT name FROM user ORDER BY name DESC START 1 LIMIT 2")).toEqual([
			"John",
			"Jane",
		]);
		expect(await names("SELECT name FROM user ORDER BY name DESC START 1")).toEqual([
			"John",
			"Jane",
			"Amos",
		]);
		// START past the end yields nothing.
		expect(await names("SELECT name FROM user ORDER BY name DESC START 4")).toEqual([]);
		expect(await names("SELECT name FROM user ORDER BY name DESC LIMIT 2")).toEqual([
			"Zoey",
			"John",
		]);

		await db.close();
	});
});

test("record-id range SELECTs honor inclusive and exclusive bounds", async () => {
	// Ported from basic.rs `select_record_ranges`.
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query("CREATE user:amos; CREATE user:jane; CREATE user:john; CREATE user:zoey;")
			.collect();

		const ids = async (q: string) => {
			const [rows] = (await db.query(q).json()) as [Array<{ id: string }>];
			return rows.map((r) => r.id);
		};

		expect(await ids("SELECT id FROM user:..")).toEqual([
			"user:amos",
			"user:jane",
			"user:john",
			"user:zoey",
		]);
		// `..john` is exclusive of the upper bound; `..=john` is inclusive.
		expect(await ids("SELECT id FROM user:..john")).toEqual(["user:amos", "user:jane"]);
		expect(await ids("SELECT id FROM user:..=john")).toEqual([
			"user:amos",
			"user:jane",
			"user:john",
		]);
		// The lower bound is inclusive by default.
		expect(await ids("SELECT id FROM user:jane..")).toEqual([
			"user:jane",
			"user:john",
			"user:zoey",
		]);
		expect(await ids("SELECT id FROM user:jane..john")).toEqual(["user:jane"]);
		expect(await ids("SELECT id FROM user:jane..=john")).toEqual(["user:jane", "user:john"]);

		await db.close();
	});
});

test("FETCH resolves linked record ids into inline objects", async () => {
	// Ported from basic.rs `select_records_fetch`.
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE tag:rs SET name = 'Rust';
				 CREATE tag:go SET name = 'Golang';
				 CREATE tag:js SET name = 'JavaScript';
				 CREATE person:tobie SET tags = [tag:rs, tag:go, tag:js];
				 CREATE person:jaime SET tags = [tag:js];`,
			)
			.collect();

		const [all] = (await db.query("SELECT * FROM person ORDER BY id FETCH tags").json()) as [
			Array<{ id: string; tags: Array<{ id: string; name: string }> }>,
		];
		expect(all).toEqual([
			{ id: "person:jaime", tags: [{ id: "tag:js", name: "JavaScript" }] },
			{
				id: "person:tobie",
				tags: [
					{ id: "tag:rs", name: "Rust" },
					{ id: "tag:go", name: "Golang" },
					{ id: "tag:js", name: "JavaScript" },
				],
			},
		]);

		// LIMIT 1 (default id order) fetches only the first person, tags inlined.
		const [limited] = (await db.query("SELECT * FROM person LIMIT 1 FETCH tags").json()) as [
			Array<{ id: string; tags: Array<{ id: string; name: string }> }>,
		];
		expect(limited).toEqual([
			{ id: "person:jaime", tags: [{ id: "tag:js", name: "JavaScript" }] },
		]);

		await db.close();
	});
});

test("DELETE ... range returns the deleted rows and leaves the rest", async () => {
	// Ported from basic.rs `delete_record_range`.
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE user:amos SET name = 'Amos';
				 CREATE user:jane SET name = 'Jane';
				 CREATE user:john SET name = 'John';
				 CREATE user:zoey SET name = 'Zoey';`,
			)
			.collect();

		// DELETE over a [jane, zoey) range returns the two deleted rows.
		const [deleted] = (await db.query("DELETE user:jane..zoey RETURN BEFORE").json()) as [
			Array<{ id: string; name: string }>,
		];
		expect(deleted).toEqual([
			{ id: "user:jane", name: "Jane" },
			{ id: "user:john", name: "John" },
		]);

		// The rows outside the range survive.
		const [remaining] = (await db.query("SELECT * FROM user ORDER BY id").json()) as [
			Array<{ id: string; name: string }>,
		];
		expect(remaining).toEqual([
			{ id: "user:amos", name: "Amos" },
			{ id: "user:zoey", name: "Zoey" },
		]);

		await db.close();
	});
});

test("typed field coercion: TYPE decimal stores and returns a decimal", async () => {
	// Ported from basic.rs `query_decimals`.
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`DEFINE TABLE foo;
				 DEFINE FIELD bar ON foo TYPE decimal;
				 CREATE foo:x CONTENT { bar: 42.69 };`,
			)
			.collect();

		// The field is stored as a decimal (not a float): type::of reports
		// "decimal" and .json() renders the value as its exact string form.
		const [rows] = (await db
			.query("SELECT bar, type::of(bar) AS t FROM foo:x")
			.json()) as [Array<{ bar: string; t: string }>];
		expect(rows).toEqual([{ bar: "42.69", t: "decimal" }]);

		await db.close();
	});
});

test("UPDATE ... CONTENT replaces the whole record body", async () => {
	// Ported from basic.rs `update_table_with_content`.
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE user:a SET name = 'A', extra = 1;
				 CREATE user:b SET name = 'B', extra = 2;`,
			)
			.collect();

		// CONTENT is a full replace: pre-existing fields not in the new content
		// (here `extra`) are dropped from every affected row.
		const [updated] = (await db.query("UPDATE user CONTENT { name: 'X' } RETURN AFTER").json()) as [
			Array<Record<string, unknown>>,
		];
		expect(updated).toEqual([
			{ id: "user:a", name: "X" },
			{ id: "user:b", name: "X" },
		]);

		await db.close();
	});
});
