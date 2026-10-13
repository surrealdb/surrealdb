import { afterAll, beforeAll, expect, test } from "bun:test";
import {
	AlreadyExistsError,
	QueryError,
	RecordId,
	Table,
	ThrownError,
	ValidationError,
	isRetryableConflict,
} from "surrealdb";
import { rootClient, startServer, type TestServer } from "../src/harness";

// Transactions over the wire: implicit per-statement transactions, explicit
// BEGIN/COMMIT/CANCEL blocks inside a single query() call, and the SDK's
// cross-call transaction API (beginTransaction / commit / cancel), all over
// WebSocket RPC.
//
// Note: a SELECT from a table that has never existed rejects with NotFound
// ("The table 'x' does not exist") instead of returning []. Every rollback
// assertion below therefore DEFINEs the table outside the transaction first,
// so an empty read proves the rollback.

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

test("multi-statement query() without BEGIN is NOT atomic: writes around a failing statement persist", async () => {
	const { db } = await rootClient(server);

	// Statement 2 fails (duplicate record id); statements 1 and 3 still apply.
	const res = await db
		.query("CREATE ta:1 SET n = 1; CREATE ta:1 SET n = 2; CREATE ta:2 SET n = 3")
		.responses();
	expect(res).toHaveLength(3);
	expect(res[0].success).toBe(true);
	expect(res[1].success).toBe(false);
	if (!res[1].success) {
		expect(res[1].error).toBeInstanceOf(AlreadyExistsError);
		expect(res[1].error.message).toBe("Database record `ta:1` already exists");
	}
	expect(res[2].success).toBe(true);

	// Each statement ran in its own implicit transaction: no rollback happened.
	const [rows] = await db.query<[Array<{ n: number }>]>("SELECT * FROM ta ORDER BY n").json();
	expect(rows.map((r) => r.n)).toEqual([1, 3]);

	// Awaiting (or .collect()-ing) the same batch rejects because one statement
	// failed — but the successful statements' writes still persisted. The
	// thrown error is the failing statement's own error.
	const err = await rejects(db.query("CREATE ta:3 SET n = 4; CREATE ta:1 SET n = 5").collect());
	expect(err).toBeInstanceOf(AlreadyExistsError);
	const [after] = await db.query<[unknown[]]>("SELECT * FROM ta:3").json();
	expect(after).toHaveLength(1);

	await db.close();
});

test("BEGIN/COMMIT in one query() call: both writes commit, and BEGIN/COMMIT occupy response slots", async () => {
	const { db } = await rootClient(server);

	const res = await db
		.query("BEGIN; CREATE tb:1 SET n = 1; CREATE tb:2 SET n = 2; COMMIT")
		.responses();
	// BEGIN and COMMIT are statements too: 4 responses, all successful.
	expect(res).toHaveLength(4);
	expect(res.every((r) => r.success)).toBe(true);

	// In .json()/.collect() mapping the BEGIN/COMMIT slots surface as undefined.
	const rows = await db.query("BEGIN; RETURN 7; COMMIT").json();
	expect(rows).toHaveLength(3);
	expect(rows[0]).toBeUndefined();
	expect(rows[1]).toBe(7);
	expect(rows[2]).toBeUndefined();

	const [check] = await db.query<[Array<{ n: number }>]>("SELECT * FROM tb ORDER BY n").json();
	expect(check.map((r) => r.n)).toEqual([1, 2]);

	await db.close();
});

test("a failing statement inside BEGIN/COMMIT rolls back the whole transaction", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE tc");

	const res = await db
		.query("BEGIN; CREATE tc:1 SET n = 1; CREATE tc:1 SET n = 2; CREATE tc:2 SET n = 3; COMMIT")
		.responses();
	expect(res).toHaveLength(5);

	// Observed error surface across the slots:
	// - BEGIN itself still reports success,
	// - the statement that executed successfully BEFORE the failure is
	//   retroactively reported as NotExecuted ("failed transaction"),
	// - the failing statement carries its real error,
	// - statements after the failure report Cancelled,
	// - COMMIT reports "Cannot COMMIT: the transaction was aborted due to a
	//   prior error" (NotExecuted).
	expect(res[0].success).toBe(true);
	expect(res[1].success).toBe(false);
	if (!res[1].success) {
		expect(res[1].error).toBeInstanceOf(QueryError);
		expect((res[1].error as QueryError).isNotExecuted).toBe(true);
		expect(res[1].error.message).toBe("The query was not executed due to a failed transaction");
	}
	expect(res[2].success).toBe(false);
	if (!res[2].success) {
		expect(res[2].error).toBeInstanceOf(AlreadyExistsError);
		expect(res[2].error.message).toBe("Database record `tc:1` already exists");
	}
	expect(res[3].success).toBe(false);
	if (!res[3].success) {
		expect(res[3].error).toBeInstanceOf(QueryError);
		expect((res[3].error as QueryError).isCancelled).toBe(true);
	}
	expect(res[4].success).toBe(false);
	if (!res[4].success) {
		expect(res[4].error.message).toBe(
			"Cannot COMMIT: the transaction was aborted due to a prior error",
		);
	}

	// Nothing persisted — including the statement that had succeeded.
	const [rows] = await db.query<[unknown[]]>("SELECT * FROM tc").json();
	expect(rows).toHaveLength(0);

	await db.close();
});

test("CANCEL discards all writes made in the transaction", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE td");

	const res = await db.query("BEGIN; CREATE td:1 SET n = 1; CANCEL").responses();
	expect(res).toHaveLength(3);
	// BEGIN and CANCEL succeed; the write inside reports Cancelled — so a
	// plain await of a cancelled transaction rejects.
	expect(res[0].success).toBe(true);
	expect(res[1].success).toBe(false);
	if (!res[1].success) {
		expect(res[1].error).toBeInstanceOf(QueryError);
		expect((res[1].error as QueryError).isCancelled).toBe(true);
		expect(res[1].error.message).toBe(
			"The query was not executed due to a cancelled transaction",
		);
	}
	expect(res[2].success).toBe(true);

	const [rows] = await db.query<[unknown[]]>("SELECT * FROM td").json();
	expect(rows).toHaveLength(0);

	await db.close();
});

test("THROW inside BEGIN/COMMIT rolls back prior statements", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE te");

	const res = await db.query("BEGIN; CREATE te:1 SET n = 1; THROW 'boom'; COMMIT").responses();
	expect(res).toHaveLength(4);
	expect(res[1].success).toBe(false);
	if (!res[1].success) {
		// The pre-THROW write is reported as NotExecuted, like any aborted txn.
		expect((res[1].error as QueryError).isNotExecuted).toBe(true);
	}
	expect(res[2].success).toBe(false);
	if (!res[2].success) {
		expect(res[2].error).toBeInstanceOf(ThrownError);
		expect(res[2].error.message).toBe("An error occurred: boom");
	}
	expect(res[3].success).toBe(false);

	// Surprising but observed: a plain await rejects with the FIRST failed
	// slot — the NotExecuted QueryError for the rolled-back CREATE — not the
	// ThrownError itself. Use .responses() to see the actual THROW error.
	const err = await rejects(db.query("BEGIN; CREATE te:2 SET n = 1; THROW 'boom2'; COMMIT").collect());
	expect(err).toBeInstanceOf(QueryError);
	expect((err as QueryError).isNotExecuted).toBe(true);

	const [rows] = await db.query<[unknown[]]>("SELECT * FROM te").json();
	expect(rows).toHaveLength(0);

	await db.close();
});

test("RETURN inside a transaction exits the block early but still commits prior writes", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE tf");

	// 5 statements, but only 4 responses: the statement AFTER the RETURN is
	// never executed and gets no response slot at all.
	// .json() before .responses() maps rich values (RecordId) to JSON strings.
	const res = await db
		.query("BEGIN; CREATE tf:1 SET n = 1; RETURN 'early'; CREATE tf:2 SET n = 2; COMMIT")
		.json()
		.responses();
	expect(res).toHaveLength(4);
	expect(res.every((r) => r.success)).toBe(true);
	expect(res[1].success && res[1].result).toEqual([{ id: "tf:1", n: 1 }]);
	expect(res[2].success && res[2].result).toBe("early");

	// The transaction committed: the pre-RETURN write persisted, the
	// post-RETURN statement never ran.
	const [rows] = await db.query<[Array<{ id: string }>]>("SELECT * FROM tf").json();
	expect(rows.map((r) => r.id)).toEqual(["tf:1"]);

	await db.close();
});

test("beginTransaction(): read-your-write inside the txn; invisible outside until commit", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE tg");

	const txn = await db.beginTransaction();
	const [created] = await txn.query<[unknown[]]>("CREATE tg:1 SET n = 1").json();
	expect(created).toHaveLength(1);

	// Read-your-write inside the transaction.
	const [inside] = await txn.query<[unknown[]]>("SELECT * FROM tg").json();
	expect(inside).toHaveLength(1);

	// Snapshot isolation: the main session neither sees the write nor blocks.
	const [outside] = await db.query<[unknown[]]>("SELECT * FROM tg").json();
	expect(outside).toHaveLength(0);

	await txn.commit();
	const [after] = await db.query<[unknown[]]>("SELECT * FROM tg").json();
	expect(after).toHaveLength(1);

	// The committed transaction is gone: further use reports "Transaction not
	// found" (a ValidationError with InvalidParams), for query and commit alike.
	const errQuery = await rejects(txn.query("SELECT * FROM tg").collect());
	expect(errQuery).toBeInstanceOf(ValidationError);
	expect(errQuery.message).toBe("Transaction not found");
	const errCommit = await rejects(txn.commit());
	expect(errCommit).toBeInstanceOf(ValidationError);
	expect(errCommit.message).toBe("Transaction not found");

	await db.close();
});

test("beginTransaction() + cancel(): writes are discarded and the txn becomes unusable", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE th");

	const txn = await db.beginTransaction();
	await txn.query("CREATE th:1 SET n = 1").collect();
	await txn.cancel();

	const [rows] = await db.query<[unknown[]]>("SELECT * FROM th").json();
	expect(rows).toHaveLength(0);

	const err = await rejects(txn.query("SELECT * FROM th").collect());
	expect(err).toBeInstanceOf(ValidationError);
	expect(err.message).toBe("Transaction not found");

	await db.close();
});

test("concurrent transactions writing the same record: first commit wins, second commit conflicts", async () => {
	const { db } = await rootClient(server);
	await db.query("CREATE ti:1 SET n = 0");

	const ta = await db.beginTransaction();
	const tb = await db.beginTransaction();

	// Optimistic concurrency: both writes succeed locally without blocking.
	const [wa] = await ta.query<[unknown[]]>("UPSERT ti:1 SET who = 'a'").json();
	expect(wa).toHaveLength(1);
	const [wb] = await tb.query<[unknown[]]>("UPSERT ti:1 SET who = 'b'").json();
	expect(wb).toHaveLength(1);

	await ta.commit();

	// The losing commit fails with a structured TransactionConflict error that
	// isRetryableConflict() recognizes, so the SDK's default .retry() can
	// recover it.
	const err = await rejects(tb.commit());
	expect(err).toBeInstanceOf(QueryError);
	expect((err as QueryError).isTransactionConflict).toBe(true);
	expect(isRetryableConflict(err)).toBe(true);
	expect((err as QueryError).message).toBe(
		"There was a problem with the key-value store: Transaction conflict: Write conflict, retry the transaction. This transaction can be retried",
	);

	// The loser's write was discarded entirely.
	const [final] = await db.query<[Array<{ who: string }>]>("SELECT * FROM ti:1").json();
	expect(final).toHaveLength(1);
	expect(final[0].who).toBe("a");

	await db.close();
});

// The SDK's bound CRUD verbs (create / update / merge / delete / insert /
// relate / select) each carry the transaction's UUID, so a write made through
// them stays inside the transaction: read-your-write holds on the txn handle,
// but the outer session sees nothing until commit(). Note there is no standalone
// merge() verb — merge is a builder on update()/upsert(). Bound verbs resolve to
// rich values (RecordId etc.); .json() maps them to plain shapes below.

test("txn.create(): visible to txn.select, invisible to outer session until commit", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE tj");

	const txn = await db.beginTransaction();

	// create(recordId) resolves to a single record (not an array).
	const created = await txn.create(new RecordId("tj", 1)).content({ n: 1 }).json();
	expect(created).toEqual({ id: "tj:1", n: 1 });

	// Read-your-write inside the txn via the bound select verb.
	const inside = await txn.select(new RecordId("tj", 1)).json();
	expect(inside).toEqual({ id: "tj:1", n: 1 });

	// The outer session's bound select carries no txn UUID and sees nothing.
	const outside = await db.select(new Table("tj")).json();
	expect(outside).toHaveLength(0);

	await txn.commit();
	const after = await db.select(new Table("tj")).json();
	expect(after).toEqual([{ id: "tj:1", n: 1 }]);

	await db.close();
});

test("txn.update / txn.update().merge(): changes invisible outside until commit", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE tk; CREATE tk:1 SET label = 'orig1'; CREATE tk:2 SET label = 'orig2'");

	const txn = await db.beginTransaction();

	// content() replaces the whole record; merge() keeps existing fields.
	const upd = await txn.update(new RecordId("tk", 1)).content({ label: "new1", extra: 1 }).json();
	expect(upd).toEqual({ id: "tk:1", label: "new1", extra: 1 });
	const mrg = await txn.update(new RecordId("tk", 2)).merge({ merged: true }).json();
	expect(mrg).toEqual({ id: "tk:2", label: "orig2", merged: true });

	// The outer session still sees the untouched originals.
	const [outside] = await db
		.query<[Array<Record<string, unknown>>]>("SELECT * FROM tk ORDER BY id")
		.json();
	expect(outside).toEqual([
		{ id: "tk:1", label: "orig1" },
		{ id: "tk:2", label: "orig2" },
	]);

	await txn.commit();
	const [after] = await db
		.query<[Array<Record<string, unknown>>]>("SELECT * FROM tk ORDER BY id")
		.json();
	expect(after).toEqual([
		{ id: "tk:1", label: "new1", extra: 1 },
		{ id: "tk:2", label: "orig2", merged: true },
	]);

	await db.close();
});

test("txn.delete(): the row is gone inside the txn but still visible outside until commit", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE tl; CREATE tl:1 SET n = 1");

	const txn = await db.beginTransaction();

	// delete(recordId) returns the deleted record.
	const del = await txn.delete(new RecordId("tl", 1)).json();
	expect(del).toEqual({ id: "tl:1", n: 1 });

	// Inside the txn the record is gone: select(recordId) resolves to undefined.
	const insideSel = await txn.select(new RecordId("tl", 1)).json();
	expect(insideSel).toBeUndefined();

	// The outer session still sees the row.
	const [outside] = await db.query<[unknown[]]>("SELECT * FROM tl").json();
	expect(outside).toHaveLength(1);

	await txn.commit();
	const [after] = await db.query<[unknown[]]>("SELECT * FROM tl").json();
	expect(after).toHaveLength(0);

	await db.close();
});

test("txn.insert() (batch): rows invisible outside until commit", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE tm");

	const txn = await db.beginTransaction();

	const ins = await txn
		.insert(new Table("tm"), [
			{ id: new RecordId("tm", 1), a: 1 },
			{ id: new RecordId("tm", 2), a: 2 },
		])
		.json();
	expect(ins).toEqual([
		{ id: "tm:1", a: 1 },
		{ id: "tm:2", a: 2 },
	]);

	const [outside] = await db.query<[unknown[]]>("SELECT * FROM tm").json();
	expect(outside).toHaveLength(0);

	await txn.commit();
	const [after] = await db.query<[unknown[]]>("SELECT * FROM tm ORDER BY id").json();
	expect(after).toHaveLength(2);

	await db.close();
});

test("txn.relate(): the edge is invisible outside until commit", async () => {
	const { db } = await rootClient(server);
	await db.query(
		"DEFINE TABLE tn; DEFINE TABLE tn_edge TYPE RELATION; CREATE tn:a; CREATE tn:b",
	);

	const txn = await db.beginTransaction();

	// A RecordId edge argument pins the edge id; the result carries in/out links.
	const rel = await txn
		.relate(new RecordId("tn", "a"), new RecordId("tn_edge", "e1"), new RecordId("tn", "b"))
		.json();
	expect(rel).toEqual({ id: "tn_edge:e1", in: "tn:a", out: "tn:b" });

	const [outside] = await db.query<[unknown[]]>("SELECT * FROM tn_edge").json();
	expect(outside).toHaveLength(0);

	await txn.commit();
	const [after] = await db.query<[unknown[]]>("SELECT * FROM tn_edge").json();
	expect(after).toHaveLength(1);

	await db.close();
});

test("cancel() discards a bound-verb write", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE tp");

	const txn = await db.beginTransaction();
	const created = await txn.create(new RecordId("tp", 1)).content({ n: 1 }).json();
	expect(created).toEqual({ id: "tp:1", n: 1 });

	// Read-your-write holds inside the txn before cancel.
	const [inside] = await txn.query<[unknown[]]>("SELECT * FROM tp").json();
	expect(inside).toHaveLength(1);

	await txn.cancel();

	const [after] = await db.query<[unknown[]]>("SELECT * FROM tp").json();
	expect(after).toHaveLength(0);

	await db.close();
});
