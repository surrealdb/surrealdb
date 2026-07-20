import { afterAll, beforeAll, expect, test } from "bun:test";
import {
	DateTime,
	Decimal,
	Duration,
	RecordId,
	StringRecordId,
	Uuid,
} from "surrealdb";
import { rootClient, startServer, type TestServer } from "../src/harness";

// SurrealQL semantics at the wire/SDK boundary: rich-type CBOR roundtrips,
// binding scope, and value-mapping edges. Multi-statement / transaction
// response anatomy (per-statement isolation, BEGIN/COMMIT retro-failure)
// is pinned in tests/transactions.test.ts.
//
// NOTE on error assertions: errors surfaced by surrealdb.js carry the
// right constructor *name* (ThrownError, QueryError, ...) but fail
// `instanceof` checks against the classes exported from the package root
// (two copies of the class hierarchy exist in the bundle). We therefore
// match on `constructor.name` + message throughout.

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

test("SELECT from a never-created table is a loud NotFoundError, not an empty result", async () => {
	const { db } = await rootClient(server);

	// Pinned as observed on 3.x (differs from older-major behavior): a table
	// that has never existed — including one whose only CREATE was rolled
	// back — rejects with NotFoundError instead of returning []. Rollback
	// assertions elsewhere in the suite must DEFINE TABLE outside the
	// transaction for this reason (see tests/transactions.test.ts).
	const err = await rejects(db.query("SELECT * FROM never_created").collect());
	expect(err.constructor.name).toBe("NotFoundError");
	expect(String(err)).toMatch(/table 'never_created' does not exist/i);

	await db.close();
});

test("rich types roundtrip through CBOR bindings with class identity intact", async () => {
	const { db } = await rootClient(server);

	const rid = new RecordId("thing", "alpha");
	const date = new Date("2026-07-17T12:34:56.789Z"); // plain JS Date in
	const uuid = new Uuid("018f2a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b");
	const dur = new Duration("1h30m");
	const dec = new Decimal("12345678901234567890.123456789"); // > f64 precision
	const nested = { a: { b: [1, "two", { c: true }] } };
	const arr = [1, "x", null];
	const bindings = { rid, dt: date, uuid, dur, dec, nested, arr };

	// The server sees the correct SurrealQL types...
	const [types] = await db
		.query<[Record<string, string>]>(
			`RETURN {
				rid: type::of($rid), dt: type::of($dt), uuid: type::of($uuid),
				dur: type::of($dur), dec: type::of($dec),
				nested: type::of($nested), arr: type::of($arr)
			}`,
			bindings,
		)
		.json();
	expect(types).toEqual({
		rid: "record",
		dt: "datetime",
		uuid: "uuid",
		dur: "duration",
		dec: "decimal",
		nested: "object",
		arr: "array",
	});

	// ...and the values come back as the SDK's value classes.
	const [out] = (await db.query(
		"RETURN { rid: $rid, dt: $dt, uuid: $uuid, dur: $dur, dec: $dec, nested: $nested, arr: $arr }",
		bindings,
	)) as [Record<string, unknown>];

	expect(out.rid).toBeInstanceOf(RecordId);
	expect(String(out.rid)).toBe("thing:alpha");
	// A bound JS Date returns as the SDK's DateTime, NOT a JS Date.
	expect(out.dt).toBeInstanceOf(DateTime);
	expect(out.dt).not.toBeInstanceOf(Date);
	expect((out.dt as DateTime).toISOString()).toBe("2026-07-17T12:34:56.789Z");
	expect(out.uuid).toBeInstanceOf(Uuid);
	expect(String(out.uuid)).toBe("018f2a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b");
	expect(out.dur).toBeInstanceOf(Duration);
	expect(String(out.dur)).toBe("1h30m");
	// Decimal precision survives exactly (beyond f64).
	expect(out.dec).toBeInstanceOf(Decimal);
	expect(String(out.dec)).toBe("12345678901234567890.123456789");
	expect(out.nested).toEqual(nested);
	expect(out.arr).toEqual(arr);

	// .json() flattens all of them to plain strings / JSON values.
	const [jout] = await db
		.query<[Record<string, unknown>]>(
			"RETURN { rid: $rid, dt: $dt, uuid: $uuid, dur: $dur, dec: $dec }",
			bindings,
		)
		.json();
	expect(jout).toEqual({
		rid: "thing:alpha",
		dt: "2026-07-17T12:34:56.789Z",
		uuid: "018f2a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
		dur: "1h30m",
		dec: "12345678901234567890.123456789",
	});

	await db.close();
});

test("complex record ids (array and object id parts) roundtrip through bindings", async () => {
	const { db } = await rootClient(server);

	const arrId = new RecordId("multi", ["2026-07-17", 42]);
	const objId = new RecordId("multi", { region: "eu", seq: 7 });
	await db.query("CREATE $a; CREATE $b;", { a: arrId, b: objId });

	const [rows] = (await db.query("SELECT * FROM $a", { a: arrId })) as [
		Array<{ id: RecordId }>,
	];
	expect(rows).toHaveLength(1);
	const got = rows[0].id;
	expect(got).toBeInstanceOf(RecordId);
	expect(got.table.name).toBe("multi");
	// The array id part comes back structurally intact.
	expect(got.id).toEqual(["2026-07-17", 42]);
	// NOTE: got.equals(arrId) is NOT asserted here — surrealdb.js
	// crashes with "Cannot access invalid private field" when comparing a
	// decoded RecordId with a locally constructed one (two class copies in
	// the bundle). SDK bug, not server behavior.

	const [objRows] = (await db.query("SELECT * FROM $b", { b: objId })) as [
		Array<{ id: RecordId }>,
	];
	expect(objRows).toHaveLength(1);
	expect(objRows[0].id.id).toEqual({ region: "eu", seq: 7 });

	await db.close();
});

test("strings that look like record ids / datetimes / durations stay strings when bound as strings", async () => {
	const { db } = await rootClient(server);

	const bindings = {
		ridish: "thing:alpha",
		dateish: "2026-07-17T12:34:56Z",
		durish: "1h30m",
		uuidish: "018f2a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
	};

	// No implicit re-parsing server-side: all remain type "string".
	const [types] = await db
		.query<[Record<string, string>]>(
			`RETURN {
				ridish: type::of($ridish), dateish: type::of($dateish),
				durish: type::of($durish), uuidish: type::of($uuidish)
			}`,
			bindings,
		)
		.json();
	expect(types).toEqual({
		ridish: "string",
		dateish: "string",
		durish: "string",
		uuidish: "string",
	});

	// And they come back as identical JS strings, even after a storage trip.
	const [rec] = (await db.query("CREATE str_probe:one CONTENT $b RETURN AFTER", {
		b: bindings,
	})) as [Array<Record<string, unknown>>];
	expect(rec[0].ridish).toBe("thing:alpha");
	expect(typeof rec[0].ridish).toBe("string");
	expect(rec[0].dateish).toBe("2026-07-17T12:34:56Z");
	expect(typeof rec[0].dateish).toBe("string");

	// Contrast: StringRecordId explicitly opts in to record-id parsing.
	const [srid] = await db
		.query<[string]>("RETURN type::of($s)", { s: new StringRecordId("thing:alpha") })
		.json();
	expect(srid).toBe("record");

	await db.close();
});

test("query bindings vs LET: binding is visible before the LET, shadowed after", async () => {
	const { db } = await rootClient(server);

	const responses = await db
		.query("RETURN $x; LET $x = 'from-let'; RETURN $x;", { x: "from-binding" })
		.responses<[string, undefined, string]>();

	expect(responses).toHaveLength(3);
	// Before the LET, the wire binding is in scope.
	expect(responses[0].success).toBe(true);
	if (responses[0].success) expect(responses[0].result).toBe("from-binding");
	// The LET statement itself succeeds with an empty (undefined) result.
	expect(responses[1].success).toBe(true);
	if (responses[1].success) expect(responses[1].result).toBeUndefined();
	// After the LET, the LET value shadows the binding.
	expect(responses[2].success).toBe(true);
	if (responses[2].success) expect(responses[2].result).toBe("from-let");

	// Unrelated bindings are unaffected in the same call.
	const [other] = await db
		.query<[string]>("LET $x = 'shadow'; RETURN $y;", { x: "b", y: "bind-only" })
		.json()
		.collect(1);
	expect(other).toBe("bind-only");

	await db.close();
});

test("protected params: $auth/$session/$token bindings rejected; $this/$parent accepted but shadowed by document context", async () => {
	const { db } = await rootClient(server);

	// Server-side rejection ("'<name>' is a protected variable and cannot be
	// set") — the whole call fails.
	for (const name of ["auth", "session", "token"]) {
		const err = await rejects(db.query(`RETURN $${name}`, { [name]: "injected" }).collect());
		expect(err.constructor.name).toBe("ValidationError");
		expect(String(err)).toMatch(new RegExp(`'${name}' is a protected variable`));
	}

	// Surprising but observed: $this and $parent are NOT protected — a wire
	// binding for them is accepted and readable at the top level. Pinned.
	const [topThis] = await db.query<[string]>("RETURN $this", { this: "injected" }).json();
	expect(topThis).toBe("injected");
	const [topParent] = await db.query<[string]>("RETURN $parent", { parent: "injected" }).json();
	expect(topParent).toBe("injected");

	// Inside per-record evaluation the document context wins over the
	// injected binding: $this is the current record, not "injected".
	await db.query("CREATE scope_probe:one SET n = 1");
	const [rows] = await db
		.query<[Array<{ me: unknown; n: number }>]>(
			"SELECT $this AS me, n FROM scope_probe",
			{ this: "injected" },
		)
		.json();
	expect(rows).toEqual([{ me: { id: "scope_probe:one", n: 1 }, n: 1 }]);

	// Likewise $parent inside a subquery refers to the outer record.
	const [sub] = await db
		.query<[Array<{ sub: Array<{ pn: number }>; n: number }>]>(
			"SELECT (SELECT $parent.n AS pn FROM scope_probe) AS sub, n FROM scope_probe",
			{ parent: "injected" },
		)
		.json();
	expect(sub).toEqual([{ sub: [{ pn: 1 }], n: 1 }]);

	await db.close();
});

test("a very large single response (1000 records) arrives intact", async () => {
	const { db } = await rootClient(server);

	const [created] = await db
		.query<[unknown[]]>("CREATE |big:1000| SET n = 7 RETURN NONE")
		.collect();
	expect(created).toHaveLength(0); // RETURN NONE: created rows not echoed

	const [rows] = (await db.query("SELECT * FROM big")) as [
		Array<{ id: RecordId; n: number }>,
	];
	expect(rows).toHaveLength(1000);
	// Every row decoded with a proper RecordId and its payload intact.
	const ids = new Set<string>();
	for (const row of rows) {
		expect(row.id).toBeInstanceOf(RecordId);
		expect(row.n).toBe(7);
		ids.add(String(row.id));
	}
	expect(ids.size).toBe(1000);

	const [count] = await db.query<[number]>("RETURN count(SELECT 1 FROM big)").json();
	expect(count).toBe(1000);

	await db.close();
});

test("UTF-8, emoji, and nul-adjacent strings roundtrip in keys and values", async () => {
	const { db } = await rootClient(server);

	const value =
		"héllo \u{1F44B}\u{1F30D} \u0001\u001f \\n literal \n newline \t tab 中文 \u{1F1EC}\u{1F1E7}";
	const key = "ключ\u{1F511}";
	const nul = "before\u0000after";

	// Through RETURN...
	const payload: Record<string, string> = { plain: value, nul };
	payload[key] = "keyed";
	const [echo] = (await db.query("RETURN $o", { o: payload })) as [
		Record<string, string>,
	];
	expect(echo.plain).toBe(value);
	expect(echo.nul).toBe(nul);
	expect(echo[key]).toBe("keyed");

	// ...and through an actual storage roundtrip, including as a record id part.
	const rid = new RecordId("utf8_probe", key);
	await db.query("CREATE $rid CONTENT $o", { rid, o: payload });
	const [rows] = (await db.query("SELECT * FROM $rid", { rid })) as [
		Array<Record<string, unknown>>,
	];
	expect(rows).toHaveLength(1);
	expect(rows[0].plain).toBe(value);
	expect(rows[0].nul).toBe(nul);
	expect(rows[0][key]).toBe("keyed");
	expect((rows[0].id as RecordId).id).toBe(key);

	await db.close();
});

test("NONE vs NULL distinction survives both typed collect and .json()", async () => {
	const { db } = await rootClient(server);

	// Typed collect: NONE -> undefined, NULL -> null.
	const typed = await db.query("RETURN NONE; RETURN NULL;");
	expect(typed).toHaveLength(2);
	expect(typed[0]).toBeUndefined();
	expect(typed[1]).toBeNull();

	// .json(): same mapping.
	const json = await db.query("RETURN NONE; RETURN NULL;").json();
	expect(json).toHaveLength(2);
	expect(json[0]).toBeUndefined();
	expect(json[1]).toBeNull();

	// A JS `undefined` binding is transmitted as NONE, `null` as NULL.
	const [kinds] = await db
		.query<[[string, string]]>("RETURN [type::of($u), type::of($n)]", {
			u: undefined,
			n: null,
		})
		.json();
	expect(kinds).toEqual(["none", "null"]);

	// Inside a record, a NONE-valued field is absent from the stored object.
	const [rec] = await db
		.query<[Array<Record<string, unknown>>]>(
			"CREATE nn_probe:one SET a = NONE, b = NULL RETURN AFTER",
		)
		.json();
	expect(rec[0]).toEqual({ id: "nn_probe:one", b: null });
	expect("a" in rec[0]).toBe(false);

	await db.close();
});
