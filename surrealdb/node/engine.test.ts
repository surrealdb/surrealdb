/**
 * Smoke tests for the built `@surrealdb/node` package.
 *
 * These drive the published artefact — `dist/surrealdb-node.mjs` and the
 * native addon beside it — through the real SDK, so they cover the three NAPI
 * entry points the engine exposes: the RPC channel, the notification channel,
 * and the export/import pair. Run `bun run build` first.
 */

import { expect, test } from "bun:test";
import { Surreal, Table } from "surrealdb";
import { createNodeEngines } from "./dist/surrealdb-node.mjs";

/** Open an in-memory database, ready for queries. */
async function connect(): Promise<Surreal> {
	const db = new Surreal({ engines: createNodeEngines() });

	await db.connect("mem://");
	await db.use({ namespace: "test", database: "test" });

	return db;
}

test("reports the engine version", async () => {
	const db = await connect();

	const { version } = await db.version();
	expect(version).toStartWith("surrealdb-");

	await db.close();
});

test("creates and selects a record", async () => {
	const db = await connect();

	await db.query("CREATE person:tobie SET name = 'Tobie'");
	const [people] = await db.query<[{ name: string }[]]>("SELECT * FROM person");

	expect(people).toHaveLength(1);
	expect(people[0]?.name).toBe("Tobie");

	await db.close();
});

test("delivers live query notifications", async () => {
	const db = await connect();

	// LIVE SELECT resolves its target up front, so the table has to exist
	// before the subscription is registered.
	await db.query("DEFINE TABLE person SCHEMALESS");

	const subscription = await db.live(new Table("person"));
	const received = (async () => {
		for await (const message of subscription) {
			return message;
		}
	})();

	await db.query("CREATE person:tobie SET name = 'Tobie'");

	const message = await received;
	expect(message?.action).toBe("CREATE");

	await subscription.kill();
	await db.close();
});

test("accepts a query timeout longer than 255 seconds", async () => {
	// The timeout deserializes into a `u64`. A narrower type rejected any
	// value above 255 outright, including the one this package documents.
	const db = new Surreal({
		engines: createNodeEngines({ query_timeout: 30_000, transaction_timeout: 30_000 }),
	});

	await db.connect("mem://");
	await db.use({ namespace: "test", database: "test" });

	const [value] = await db.query<[number]>("RETURN 1");
	expect(value).toBe(1);

	await db.close();
});

test("creates the configured default namespace and database", async () => {
	const db = new Surreal({
		engines: createNodeEngines({
			defaults: { namespace: "custom", database: "custom" },
		}),
	});

	await db.connect("mem://");

	const [root] = await db.query<[{ namespaces: Record<string, string> }]>("INFO FOR ROOT");
	expect(Object.keys(root.namespaces)).toEqual(["custom"]);

	await db.use({ namespace: "custom" });

	const [namespace] = await db.query<[{ databases: Record<string, string> }]>("INFO FOR NS");
	expect(Object.keys(namespace.databases)).toEqual(["custom"]);

	await db.close();
});

test("accepts every planner strategy the options type declares", async () => {
	for (const strategy of ["best-effort", "compute-only", "all-read-only"] as const) {
		const db = new Surreal({
			engines: createNodeEngines({ capabilities: { planner_strategy: strategy } }),
		});

		await db.connect("mem://");
		await db.use({ namespace: "test", database: "test" });

		const [value] = await db.query<[number]>("RETURN 1");
		expect(value).toBe(1);

		await db.close();
	}
});

test("exports and re-imports the database", async () => {
	const db = await connect();

	await db.query("CREATE person:tobie SET name = 'Tobie'");
	const exported = await db.export();
	expect(exported).toContain("person:tobie");

	await db.query("DELETE person");
	await db.import(exported);

	const [people] = await db.query<[unknown[]]>("SELECT * FROM person");
	expect(people).toHaveLength(1);

	await db.close();
});
