import { afterAll, beforeAll, expect, test } from "bun:test";
import { AlreadyExistsError, QueryError, Surreal, isRetryableConflict } from "surrealdb";
import { rootClient, startServer, type TestServer } from "../src/harness";

// Concurrent-writer conflict behavior over WebSocket: two independent root
// connections writing the same records simultaneously. All assertions are
// invariant-style (sums / counts / final state / returned values) — never
// timing assumptions.
//
// One known limitation is exercised by the skipped test at the bottom: under
// rapid two-connection contention the memory engine intermittently misses a
// write-write conflict, silently losing an increment (and, rarely, letting two
// CREATEs of one record id both succeed).

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

/** A second root connection to the same namespace/database. */
async function secondRoot(namespace: string, database: string): Promise<Surreal> {
	const db = new Surreal();
	await db.connect(server.url, {
		namespace,
		database,
		authentication: { username: "root", password: "root" },
	});
	return db;
}

type Settled = { ok: true } | { ok: false; error: unknown };

async function settle(p: Promise<unknown>): Promise<Settled> {
	try {
		await p;
		return { ok: true };
	} catch (error) {
		return { ok: false, error };
	}
}

type IncrementResult = { ok: true; n: number } | { ok: false; error: unknown };

/** Run one `n += 1` increment and capture the post-increment value it returned. */
async function increment(db: Surreal, thing: string): Promise<IncrementResult> {
	try {
		const [rows] = (await db.query(`UPDATE ${thing} SET n += 1`).json()) as [
			Array<{ n: number }>,
		];
		if (rows.length !== 1) throw new Error(`UPDATE matched ${rows.length} records`);
		return { ok: true, n: rows[0].n };
	} catch (error) {
		return { ok: false, error };
	}
}

/**
 * Statements aborted by a conflicting transaction carry one of two messages:
 * the statement that hit the conflict gets the "Transaction conflict: Write
 * conflict…" text, statements skipped because the transaction already failed
 * get "The query was not executed due to a failed transaction".
 */
const CONFLICT_MESSAGE = /Transaction conflict|not executed due to a failed transaction/;

function expectConflictError(error: unknown) {
	expect(error).toBeInstanceOf(QueryError);
	const err = error as QueryError;
	expect(err.kind).toBe("Query");
	// A write conflict carries the structured TransactionConflict detail (wire
	// code -32009), so `isTransactionConflict` is true and the SDK's default
	// `.retry()` recognizes it.
	expect(err.details).toEqual({ kind: "TransactionConflict" });
	expect(err.isTransactionConflict).toBe(true);
	expect(err.message).toMatch(CONFLICT_MESSAGE);
}

test("parallel UPSERTs of distinct records from two connections all land", async () => {
	const { db: a, namespace, database } = await rootClient(server);
	const b = await secondRoot(namespace, database);
	await a.query("DEFINE TABLE conc_upsert;");

	// Distinct record ids on a pre-defined table never conflict: every one of
	// the 50 interleaved writers succeeds (0 failures across 250 ops when
	// probed repeatedly).
	const perConn = 25;
	const tasks: Promise<Settled>[] = [];
	for (let i = 0; i < perConn; i++) {
		tasks.push(settle(a.query(`UPSERT conc_upsert:a${i} SET n = ${i}`).collect()));
		tasks.push(settle(b.query(`UPSERT conc_upsert:b${i} SET n = ${i}`).collect()));
	}
	const results = await Promise.all(tasks);
	expect(results.filter((r) => !r.ok)).toHaveLength(0);

	const [count] = await a
		.query<[number]>("RETURN (SELECT VALUE count() FROM conc_upsert GROUP ALL)[0].count")
		.json();
	expect(count).toBe(perConn * 2);

	await a.close();
	await b.close();
});

test("distinct-record writes racing on implicit table creation: occasional conflicts, no lost records", async () => {
	const { db: a, namespace, database } = await rootClient(server);
	const b = await secondRoot(namespace, database);

	// No DEFINE TABLE: the first writers race to create the table definition
	// implicitly, and that shared write intermittently produces a handful of
	// write-conflict errors (observed 0-3 per 50). Invariant: a record exists
	// for exactly every reported success — conflicts reject loudly, nothing
	// is silently dropped.
	const perConn = 25;
	const tasks: Promise<Settled>[] = [];
	for (let i = 0; i < perConn; i++) {
		tasks.push(settle(a.query(`UPSERT conc_fresh:a${i} SET n = ${i}`).collect()));
		tasks.push(settle(b.query(`UPSERT conc_fresh:b${i} SET n = ${i}`).collect()));
	}
	const results = await Promise.all(tasks);
	const successes = results.filter((r) => r.ok).length;
	const failures = results.filter((r) => !r.ok) as Array<{ ok: false; error: unknown }>;
	expect(successes + failures.length).toBe(perConn * 2);
	for (const f of failures) expectConflictError(f.error);

	const [count] = await a
		.query<[number]>("RETURN (SELECT VALUE count() FROM conc_fresh GROUP ALL)[0].count")
		.json();
	expect(count).toBe(successes);

	await a.close();
	await b.close();
});

test(
	"contended single-record increments: conflicts surface as errors, and no phantom increments appear",
	async () => {
		const { db: a, namespace, database } = await rootClient(server);
		const b = await secondRoot(namespace, database);
		await a.query("DEFINE TABLE conc_inc; CREATE conc_inc:c SET n = 0;");

		// The server does NOT auto-retry conflicting writes: a bare
		// `UPDATE .. SET n += 1` racing a writer on another connection can fail
		// with a write-conflict error, and it is the client's job to retry.
		const perConn = 20;
		const tasks: Promise<IncrementResult>[] = [];
		for (let i = 0; i < perConn; i++) {
			tasks.push(increment(a, "conc_inc:c"));
			tasks.push(increment(b, "conc_inc:c"));
		}
		const results = await Promise.all(tasks);
		const successes = results.filter((r) => r.ok) as Array<{ ok: true; n: number }>;
		const failures = results.filter((r) => !r.ok) as Array<{ ok: false; error: unknown }>;
		expect(successes.length + failures.length).toBe(perConn * 2);
		expect(successes.length).toBeGreaterThan(0); // the first committer always wins
		for (const f of failures) expectConflictError(f.error);

		// Invariants: the final value equals the highest post-increment value
		// any success observed, and never exceeds the success count (increments
		// are never counted twice).
		//
		// Deliberately NOT asserted: `final == successes.length`. That stronger
		// invariant — every reported success is one real increment — fails
		// intermittently under the lost-update limitation exercised below.
		const [final] = await a.query<[number]>("SELECT VALUE n FROM ONLY conc_inc:c").json();
		const maxReturned = Math.max(...successes.map((s) => s.n));
		expect(final).toBe(maxReturned);
		expect(final).toBeLessThanOrEqual(successes.length);

		await a.close();
		await b.close();
	},
	15000,
);

test("writes pipelined on a single connection can also conflict: they are not serialized", async () => {
	const { db: a } = await rootClient(server);
	await a.query("DEFINE TABLE conc_pipe; CREATE conc_pipe:c SET n = 0;");

	// Same contended record, all writers sharing ONE WebSocket connection.
	// The server does NOT serialize a connection's requests — most rounds all
	// 20 land conflict-free, but some rounds drop one write with the same
	// write-conflict error as the cross-connection case. Do not design clients
	// around single-connection serialization.
	const attempts = 20;
	const tasks: Promise<IncrementResult>[] = [];
	for (let i = 0; i < attempts; i++) {
		tasks.push(increment(a, "conc_pipe:c"));
	}
	const results = await Promise.all(tasks);
	const successes = results.filter((r) => r.ok) as Array<{ ok: true; n: number }>;
	const failures = results.filter((r) => !r.ok) as Array<{ ok: false; error: unknown }>;
	expect(successes.length + failures.length).toBe(attempts);
	expect(successes.length).toBeGreaterThan(0);
	for (const f of failures) expectConflictError(f.error);

	const [n] = await a.query<[number]>("SELECT VALUE n FROM ONLY conc_pipe:c").json();
	expect(n).toBe(Math.max(...successes.map((s) => s.n)));
	expect(n).toBeLessThanOrEqual(successes.length);

	await a.close();
});

test(
	"retry() with a custom retryable predicate eliminates surfaced conflicts",
	async () => {
		const { db: a, namespace, database } = await rootClient(server);
		const b = await secondRoot(namespace, database);
		await a.query("DEFINE TABLE conc_retry; CREATE conc_retry:c SET n = 0;");

		// A custom message-matching retryable predicate eliminates every
		// surfaced conflict (the default isRetryableConflict predicate, which
		// matches the structured detail directly, is covered by the dedicated
		// conflict-detail test below).
		const retry = {
			attempts: 200,
			retryDelay: 2,
			retryDelayMax: 50,
			retryable: (e: unknown) => CONFLICT_MESSAGE.test(String((e as Error)?.message ?? "")),
		};
		const perConn = 15;
		const run = async (db: Surreal): Promise<IncrementResult> => {
			try {
				const [rows] = (await db
					.query("UPDATE conc_retry:c SET n += 1")
					.retry(retry)
					.json()) as [Array<{ n: number }>];
				return { ok: true, n: rows[0].n };
			} catch (error) {
				return { ok: false, error };
			}
		};
		const tasks: Promise<IncrementResult>[] = [];
		for (let i = 0; i < perConn; i++) {
			tasks.push(run(a));
			tasks.push(run(b));
		}
		const results = await Promise.all(tasks);
		const successes = results.filter((r) => r.ok) as Array<{ ok: true; n: number }>;
		// With retry enabled no conflict ever surfaces to the caller…
		expect(successes).toHaveLength(perConn * 2);

		// …but the total can still fall short of the attempt count under the
		// lost-update limitation exercised below, so only the value-consistency
		// invariant is pinned here.
		const [final] = await a.query<[number]>("SELECT VALUE n FROM ONLY conc_retry:c").json();
		expect(final).toBe(Math.max(...successes.map((s) => s.n)));
		expect(final).toBeLessThanOrEqual(perConn * 2);

		await a.close();
		await b.close();
	},
	20000,
);

test(
	"overlapping explicit BEGIN..COMMIT read-modify-write: exactly one commits, the loser aborts on COMMIT",
	async () => {
		const { db: a, namespace, database } = await rootClient(server);
		const b = await secondRoot(namespace, database);
		await a.query("DEFINE TABLE conc_ovl; CREATE conc_ovl:c SET n = 0;");

		// The in-transaction sleep guarantees both transactions read n = 0
		// before either commits, forcing a genuine write-write conflict
		// (deterministic across 12 probed rounds).
		// NOTE: the sleep must be a LET statement — `RETURN sleep(..)` inside
		// BEGIN..COMMIT ends the transaction early and silently skips the
		// remaining statements.
		const q = `
			BEGIN;
			LET $v = (SELECT VALUE n FROM ONLY conc_ovl:c);
			LET $pause = sleep(400ms);
			UPDATE conc_ovl:c SET n = $v + 1;
			COMMIT;
		`;
		// .responses() resolves even when statements fail, exposing per-statement rows.
		const [rowsA, rowsB] = await Promise.all([a.query(q).responses(), b.query(q).responses()]);

		const failed = (rows: typeof rowsA) => rows.some((row) => !row.success);
		expect(failed(rowsA) !== failed(rowsB)).toBe(true); // exactly one aborts
		const loser = failed(rowsA) ? rowsA : rowsB;
		const winner = failed(rowsA) ? rowsB : rowsA;

		// Winner: 5 rows (BEGIN, LET, LET, UPDATE, COMMIT), all successful.
		expect(winner.map((r) => r.success)).toEqual([true, true, true, true, true]);

		// Loser: every post-BEGIN row fails. The conflict itself is reported on
		// the COMMIT row; the preceding statements are marked "not executed".
		// `.collect()` would throw the FIRST failing row's generic error, so the
		// actual conflict text is only visible here on the last row.
		expect(loser.map((r) => r.success)).toEqual([true, false, false, false, false]);
		for (const row of loser.slice(1, 4)) {
			if (row.success) continue; // narrowed by the assertion above
			expect(row.error).toBeInstanceOf(QueryError);
			expect(row.error.details).toEqual({ kind: "NotExecuted" });
			expect(row.error.message).toBe("The query was not executed due to a failed transaction");
		}
		const commitRow = loser[4];
		if (!commitRow.success) {
			expect(commitRow.error).toBeInstanceOf(QueryError);
			// The COMMIT row carries the structured TransactionConflict detail.
			expect(commitRow.error.details).toEqual({ kind: "TransactionConflict" });
			expect(commitRow.error.message).toBe(
				"Cannot COMMIT: Transaction conflict: Write conflict, retry the transaction. This transaction can be retried",
			);
			expect((commitRow.error as QueryError).isTransactionConflict).toBe(true);
		}

		// No lost update in this shape: exactly one increment survives.
		const [n] = await a.query<[number]>("SELECT VALUE n FROM ONLY conc_ovl:c").json();
		expect(n).toBe(1);

		await a.close();
		await b.close();
	},
	20000,
);

test(
	"concurrent CREATE with the same record id: one record survives, normally exactly one writer wins",
	async () => {
		const { db: a, namespace, database } = await rootClient(server);
		const b = await secondRoot(namespace, database);
		await a.query("DEFINE TABLE conc_dupc;");

		const rounds = 6;
		for (let r = 0; r < rounds; r++) {
			const [ra, rb] = await Promise.all([
				settle(a.query(`CREATE conc_dupc:r${r} SET src = 'a'`).collect()),
				settle(b.query(`CREATE conc_dupc:r${r} SET src = 'b'`).collect()),
			]);
			const oks = [ra, rb].filter((x) => x.ok).length;
			// Dominant behavior: exactly one wins. Under the lost-update
			// limitation exercised below, a small fraction of rounds have BOTH
			// CREATEs of the same id report success — so a strict `oks == 1` here
			// would flake. The strict uniqueness assertion lives in the skipped
			// test below.
			expect(oks).toBeGreaterThanOrEqual(1);

			const [src] = await a
				.query<[string]>(`SELECT VALUE src FROM ONLY conc_dupc:r${r}`)
				.json();
			if (oks === 1) {
				// The loser fails either with the write-conflict QueryError
				// (truly concurrent commit) or the structured AlreadyExists
				// error (its transaction started after the winner committed).
				const loser = (ra.ok ? rb : ra) as { ok: false; error: unknown };
				expect(String((loser.error as Error).message)).toMatch(
					/Transaction conflict|already exists/,
				);
				// The surviving record belongs to the winner.
				expect(src).toBe(ra.ok ? "a" : "b");
			} else {
				// Both claimed success, yet only one write survived.
				expect(["a", "b"]).toContain(src);
			}
		}

		const [count] = await a
			.query<[number]>("RETURN (SELECT VALUE count() FROM conc_dupc GROUP ALL)[0].count")
			.json();
		expect(count).toBe(rounds);

		await a.close();
		await b.close();
	},
	20000,
);

test("sequential duplicate CREATE reports a structured AlreadyExists record error", async () => {
	const { db: a } = await rootClient(server);
	await a.query("CREATE conc_dupseq:one SET src = 'first'");

	// The deterministic (non-racing) shape of the duplicate-id error, as an
	// anchor for the timing-dependent branch in the concurrent test above.
	let error: unknown;
	try {
		await a.query("CREATE conc_dupseq:one SET src = 'second'").collect();
	} catch (e) {
		error = e;
	}
	expect(error).toBeInstanceOf(AlreadyExistsError);
	const err = error as AlreadyExistsError;
	expect(err.kind).toBe("AlreadyExists");
	expect(err.details).toEqual({ kind: "Record", details: { id: "conc_dupseq:one" } });
	expect(err.message).toBe("Database record `conc_dupseq:one` already exists");

	// The winner's content is untouched.
	const [src] = await a.query<[string]>("SELECT VALUE src FROM ONLY conc_dupseq:one").json();
	expect(src).toBe("first");

	await a.close();
});

// Known limitation: under rapid two-connection contention the memory engine
// intermittently misses a write-write conflict, silently losing a write. Two
// manifestations of the same defect:
//   a. `UPDATE c SET n += 1` from two connections: some rounds have two
//      successful responses returning the SAME post-increment n and a final
//      value below the success count. Client-side `.retry()` cannot paper over
//      it — the loss happens as a swallowed conflict, not a surfaced one.
//   b. Concurrent `CREATE same:id` from two connections: rarely BOTH report
//      success even though only one record survives — a record-id uniqueness
//      violation.
// The identical interleaving usually reports "Transaction conflict: Write
// conflict, retry the transaction", so detection works most of the time and
// then intermittently fails. This skipped test pins the intended serializable
// behavior.
test.skip(
	"concurrent writes are serializable: no lost increments, no double-created ids",
	async () => {
		const { db: a, namespace, database } = await rootClient(server);
		const b = await secondRoot(namespace, database);
		await a.query("DEFINE TABLE conc_lu; CREATE conc_lu:c SET n = 0;");

		// Phase 1: contended increments count exactly once.
		const perConn = 20;
		const tasks: Promise<IncrementResult>[] = [];
		for (let i = 0; i < perConn; i++) {
			tasks.push(increment(a, "conc_lu:c"));
			tasks.push(increment(b, "conc_lu:c"));
		}
		const results = await Promise.all(tasks);
		const successes = results.filter((r) => r.ok) as Array<{ ok: true; n: number }>;

		// Serializable increments: all returned post-increment values distinct…
		const returned = successes.map((s) => s.n);
		expect(new Set(returned).size).toBe(returned.length);

		// …and the final value counts every success exactly once.
		const [final] = await a.query<[number]>("SELECT VALUE n FROM ONLY conc_lu:c").json();
		expect(final).toBe(successes.length);

		// Phase 2: concurrent CREATEs of one id — exactly one may ever succeed.
		for (let r = 0; r < 20; r++) {
			const [ra, rb] = await Promise.all([
				settle(a.query(`CREATE conc_lu_dup:r${r} SET src = 'a'`).collect()),
				settle(b.query(`CREATE conc_lu_dup:r${r} SET src = 'b'`).collect()),
			]);
			expect([ra, rb].filter((x) => x.ok)).toHaveLength(1);
		}

		await a.close();
		await b.close();
	},
	30000,
);

// Transaction conflicts carry the structured `{ kind: "TransactionConflict" }`
// detail (wire code -32009), so `isRetryableConflict()` matches and the SDK's
// default `.retry()` (no custom predicate) recovers contended writes on its
// own.
test(
	"transaction conflicts carry the structured TransactionConflict detail so default retry() works",
	async () => {
		const { db: a, namespace, database } = await rootClient(server);
		const b = await secondRoot(namespace, database);
		await a.query("DEFINE TABLE conc_tc; CREATE conc_tc:c SET n = 0;");

		// With the structured detail emitted, the DEFAULT predicate must match…
		const probe: Settled[] = [];
		for (let i = 0; i < 10; i++) {
			probe.push(
				...(await Promise.all([
					settle(a.query("UPDATE conc_tc:c SET n += 1").collect()),
					settle(b.query("UPDATE conc_tc:c SET n += 1").collect()),
				])),
			);
		}
		for (const f of probe.filter((r) => !r.ok) as Array<{ ok: false; error: unknown }>) {
			expect((f.error as QueryError).isTransactionConflict).toBe(true);
			expect(isRetryableConflict(f.error)).toBe(true);
		}

		// …and default .retry() (no custom predicate) surfaces no conflicts.
		await a.query("UPDATE conc_tc:c SET n = 0");
		const perConn = 15;
		const tasks: Promise<Settled>[] = [];
		for (let i = 0; i < perConn; i++) {
			tasks.push(
				settle(a.query("UPDATE conc_tc:c SET n += 1").retry({ attempts: 200 }).collect()),
			);
			tasks.push(
				settle(b.query("UPDATE conc_tc:c SET n += 1").retry({ attempts: 200 }).collect()),
			);
		}
		const results = await Promise.all(tasks);
		expect(results.filter((r) => !r.ok)).toHaveLength(0);

		await a.close();
		await b.close();
	},
	30000,
);
