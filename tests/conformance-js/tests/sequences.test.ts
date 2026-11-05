import { afterAll, beforeAll, expect, test } from "bun:test";
import { Surreal } from "surrealdb";
import { rootClient, startServer, type TestServer } from "../src/harness";

// DEFINE SEQUENCE + sequence::nextval() conformance. The load-bearing property
// is that nextval hands out a strictly increasing, gap-free run of integers from
// START — and that a single server never hands the same value out twice, even
// when many connections pull concurrently (a duplicate id is silent data
// corruption). BATCH controls how large a range the node reserves from the KV
// store at a time; on one server every connection shares one in-process
// allocator, so BATCH never introduces gaps or duplicates in the values seen.

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

/** A second authenticated root connection to the same namespace/database. */
async function secondRoot(namespace: string, database: string): Promise<Surreal> {
	const db = new Surreal();
	await db.connect(server.url, {
		namespace,
		database,
		authentication: { username: "root", password: "root" },
	});
	return db;
}

/** Pull one sequence value. */
async function nextval(db: Surreal, name: string): Promise<number> {
	const [v] = await db.query<[number]>(`RETURN sequence::nextval('${name}')`).json();
	return v;
}

test("single-connection nextval is strictly monotonic from START with no gaps", async () => {
	const { db } = await rootClient(server);
	try {
		// BATCH 50 with 120 pulls spans three batch allocations; the values seen
		// stay contiguous across the batch boundaries — BATCH is a KV-reservation
		// size, not a stride.
		await db.query("DEFINE SEQUENCE order_id BATCH 50 START 0");
		const pulls = 120;
		const seen: number[] = [];
		for (let i = 0; i < pulls; i++) {
			seen.push(await nextval(db, "order_id"));
		}
		expect(seen).toEqual(Array.from({ length: pulls }, (_, i) => i));
	} finally {
		await db.close();
	}
}, 30000);

test("START seeds the first value (including a negative start)", async () => {
	const { db } = await rootClient(server);
	try {
		await db.query("DEFINE SEQUENCE ticket BATCH 10 START 100");
		expect(await nextval(db, "ticket")).toBe(100);
		expect(await nextval(db, "ticket")).toBe(101);

		// A negative START is honored and still increments by one.
		await db.query("DEFINE SEQUENCE offset BATCH 5 START -3");
		expect(await nextval(db, "offset")).toBe(-3);
		expect(await nextval(db, "offset")).toBe(-2);
	} finally {
		await db.close();
	}
}, 30000);

test("concurrent nextval across many connections yields a gap-free, duplicate-free union", async () => {
	const { db: a, namespace, database } = await rootClient(server);
	const connCount = 8;
	const perConn = 60;
	const others = await Promise.all(
		Array.from({ length: connCount - 1 }, () => secondRoot(namespace, database)),
	);
	const conns = [a, ...others];
	try {
		// BATCH 50 is deliberately smaller than the per-connection pull count so
		// every connection forces multiple batch reallocations while racing the
		// others on the same sequence.
		await a.query("DEFINE SEQUENCE order_id BATCH 50 START 0");

		// Each connection pulls its values sequentially (awaiting each), and all
		// connections run in parallel. Per connection the values it observes are
		// strictly increasing, because a single server serializes every allocation
		// through one in-process mutex.
		const pull = async (db: Surreal): Promise<number[]> => {
			const got: number[] = [];
			for (let i = 0; i < perConn; i++) {
				got.push(await nextval(db, "order_id"));
			}
			return got;
		};
		const perConnValues = await Promise.all(conns.map(pull));

		// Per-connection monotonicity: each connection's own draws strictly ascend.
		for (const values of perConnValues) {
			for (let i = 1; i < values.length; i++) {
				expect(values[i]).toBeGreaterThan(values[i - 1]);
			}
		}

		// The union is the exact contiguous range [0, total): no duplicates
		// (a repeat would be silent id corruption) and no gaps.
		const total = connCount * perConn;
		const all = perConnValues.flat();
		expect(all).toHaveLength(total);
		const unique = new Set(all);
		expect(unique.size).toBe(total);
		expect([...all].sort((x, y) => x - y)).toEqual(
			Array.from({ length: total }, (_, i) => i),
		);
	} finally {
		await Promise.all(conns.map((db) => db.close()));
	}
}, 60000);

test("nextval on an undefined sequence errors", async () => {
	const { db } = await rootClient(server);
	try {
		const boom = db
			.query("RETURN sequence::nextval('missing')")
			.collect()
			.then(() => null)
			.catch((e) => e as Error);
		const err = await boom;
		expect(err).toBeInstanceOf(Error);
		expect((err as Error).message).toContain("The sequence 'missing' does not exist");
	} finally {
		await db.close();
	}
}, 30000);
