// Change feeds over the wire: DEFINE TABLE ... CHANGEFEED, mutate, then
// SHOW CHANGES FOR TABLE ... SINCE ...  — ordered history with monotonic
// versionstamps, inclusive versionstamp boundaries, and the empty (not error)
// result for a table with no change feed.
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
		// whole feed (full monotonicity
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

test("changefeed INCLUDE ORIGINAL: the define_table feed entry reports changefeed original:true", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);

		// INCLUDE ORIGINAL flips the table's changefeed into store-diff mode,
		// which the recorded define_table entry advertises via `original: true`
		// (the default feed records `original: false`).
		await db.query("DEFINE TABLE t CHANGEFEED 1h INCLUDE ORIGINAL").collect();

		const [entries] = (await db
			.query("SHOW CHANGES FOR TABLE t SINCE 0 LIMIT 10")
			.json()) as [
			Array<{ versionstamp: bigint; changes: Array<Record<string, unknown>> }>,
		];

		expect(entries).toHaveLength(1);
		expect(entries[0].changes).toEqual([
			{
				define_table: {
					id: 0,
					name: "t",
					changefeed: { expiry: "1h", original: true },
					drop: false,
					kind: { kind: "ANY" },
					permissions: { create: false, delete: false, select: false, update: false },
					schemafull: false,
				},
			},
		]);

		await db.close();
	});
});

test("changefeed INCLUDE ORIGINAL: UPDATE carries the after-image plus reverse patches, DELETE carries the before-image", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);

		await db.query("DEFINE TABLE t CHANGEFEED 1h INCLUDE ORIGINAL").collect();
		await db.query("CREATE t:a SET n = 1").collect();
		await db.query("UPDATE t:a SET n = 2, extra = 'x'").collect();
		await db.query("DELETE t:a").collect();

		const [entries] = (await db
			.query("SHOW CHANGES FOR TABLE t SINCE 0 LIMIT 10")
			.json()) as [
			Array<{ versionstamp: bigint; changes: Array<Record<string, unknown>> }>,
		];

		// define_table + create + update + delete.
		expect(entries).toHaveLength(4);

		// A CREATE has no prior image, so store-diff mode still records it as a
		// plain `update` with the full after-image — there is no before-image to
		// surface.
		expect(entries[1].changes).toEqual([{ update: { id: "t:a", n: 1 } }]);

		// An UPDATE does NOT surface the before-image directly. Instead the entry
		// carries `current` (the after-image) and an `update` array of JSON-patch
		// operations recorded in REVERSE (current -> previous), so applying them
		// to `current` reconstructs the original. Ops arrive in sorted-key order:
		// `extra` (added by this UPDATE) is removed, `n` is replaced back to 1.
		expect(entries[2].changes).toEqual([
			{
				current: { id: "t:a", n: 2, extra: "x" },
				update: [
					{ op: "remove", path: "/extra" },
					{ op: "replace", path: "/n", value: 1 },
				],
			},
		]);

		// A DELETE surfaces the before-image directly: the deleted record is
		// carried verbatim under `delete.original` (the default feed records only
		// `{ delete: { id } }`).
		expect(entries[3].changes).toEqual([
			{ delete: { id: "t:a", original: { id: "t:a", n: 2, extra: "x" } } },
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

