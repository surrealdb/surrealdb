import { afterAll, beforeAll, expect, test } from "bun:test";
import {
	BoundExcluded,
	BoundIncluded,
	DateTime,
	Decimal,
	Duration,
	FileRef,
	Geometry,
	GeometryCollection,
	GeometryLine,
	GeometryMultiLine,
	GeometryMultiPoint,
	GeometryMultiPolygon,
	GeometryPoint,
	GeometryPolygon,
	Range,
	RecordId,
	RecordIdRange,
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

test("binary values roundtrip as ArrayBuffer through bindings, casts, and storage", async () => {
	const { db } = await rootClient(server);

	const bytes = new Uint8Array([0, 1, 2, 254, 255, 127, 128]);

	// The server sees SurrealQL `bytes`.
	const [t] = await db.query<[string]>("RETURN type::of($b)", { b: bytes }).json();
	expect(t).toBe("bytes");

	// Surprising but observed: a bound Uint8Array decodes back as an
	// ArrayBuffer, NOT a Uint8Array. Wrap in a view to compare contents.
	const [out] = (await db.query("RETURN $b", { b: bytes })) as [unknown];
	expect(out).toBeInstanceOf(ArrayBuffer);
	expect(out).not.toBeInstanceOf(Uint8Array);
	expect(new Uint8Array(out as ArrayBuffer)).toEqual(bytes);

	// A <bytes> cast produces the same wire type and ArrayBuffer decode.
	const [castT] = await db.query<[string]>("RETURN type::of(<bytes>'hello')").json();
	expect(castT).toBe("bytes");
	const [castOut] = (await db.query("RETURN <bytes>'hello'")) as [unknown];
	expect(castOut).toBeInstanceOf(ArrayBuffer);
	expect(new Uint8Array(castOut as ArrayBuffer)).toEqual(
		new Uint8Array([104, 101, 108, 108, 111]),
	);

	// Stored in a record and read back, still ArrayBuffer with identical bytes.
	await db.query("CREATE bin_probe:one SET data = $b", { b: bytes });
	const [rows] = (await db.query("SELECT * FROM bin_probe:one")) as [
		Array<{ data: unknown }>,
	];
	expect(rows).toHaveLength(1);
	expect(rows[0].data).toBeInstanceOf(ArrayBuffer);
	expect(new Uint8Array(rows[0].data as ArrayBuffer)).toEqual(bytes);

	await db.close();
});

test("the seven GeoJSON geometry shapes roundtrip with class and structure intact", async () => {
	const { db } = await rootClient(server);

	const p1 = new GeometryPoint([1, 2]);
	const p2 = new GeometryPoint([3, 4]);
	const p3 = new GeometryPoint([5, 6]);
	const line = new GeometryLine([p1, p2]);
	const line2 = new GeometryLine([p2, p3]);
	const ring = new GeometryLine([
		new GeometryPoint([0, 0]),
		new GeometryPoint([0, 1]),
		new GeometryPoint([1, 1]),
		new GeometryPoint([1, 0]),
		new GeometryPoint([0, 0]),
	]);
	const poly = new GeometryPolygon([ring]);
	const mpoint = new GeometryMultiPoint([p1, p2]);
	const mline = new GeometryMultiLine([line, line2]);
	const mpoly = new GeometryMultiPolygon([poly]);
	const coll = new GeometryCollection([p1, line]);

	// type::of surfaces the specific kind as `geometry<kind>`.
	const cases: Array<[Geometry, string, unknown, new (...a: never[]) => Geometry]> = [
		[p1, "geometry<point>", p1.toJSON(), GeometryPoint],
		[line, "geometry<line>", line.toJSON(), GeometryLine],
		[poly, "geometry<polygon>", poly.toJSON(), GeometryPolygon],
		[mpoint, "geometry<multipoint>", mpoint.toJSON(), GeometryMultiPoint],
		[mline, "geometry<multiline>", mline.toJSON(), GeometryMultiLine],
		[mpoly, "geometry<multipolygon>", mpoly.toJSON(), GeometryMultiPolygon],
		[coll, "geometry<collection>", coll.toJSON(), GeometryCollection],
	];

	for (const [g, kind, geojson, cls] of cases) {
		const [t] = await db.query<[string]>("RETURN type::of($g)", { g }).json();
		expect(t).toBe(kind);

		// Value class: comes back as the specific Geometry subclass.
		const [out] = (await db.query("RETURN $g", { g })) as [Geometry];
		expect(out).toBeInstanceOf(Geometry);
		expect(out).toBeInstanceOf(cls);
		expect(out.toJSON()).toEqual(geojson);

		// .json() flattens to the GeoJSON object.
		const [outJson] = await db.query("RETURN $g", { g }).json();
		expect(outJson).toEqual(geojson);
	}

	// GeoJSON `type` tags surface with their canonical spelling.
	expect((p1.toJSON() as { type: string }).type).toBe("Point");
	expect((line.toJSON() as { type: string }).type).toBe("LineString");
	expect((coll.toJSON() as { type: string }).type).toBe("GeometryCollection");

	// A geometry stored in a record survives the storage trip structurally.
	await db.query("CREATE geo_probe:one SET loc = $g", { g: poly });
	const [rows] = (await db.query("SELECT * FROM geo_probe:one")) as [
		Array<{ loc: Geometry }>,
	];
	expect(rows).toHaveLength(1);
	expect(rows[0].loc).toBeInstanceOf(GeometryPolygon);
	expect(rows[0].loc.toJSON()).toEqual(poly.toJSON());

	await db.close();
});

test("BigInt beyond 2^53 roundtrips exactly as a native bigint, including in record ids", async () => {
	const { db } = await rootClient(server);

	const big = 9007199254740993n; // 2^53 + 1 — not representable as an f64

	// The server sees SurrealQL `int`.
	const [t] = await db.query<[string]>("RETURN type::of($n)", { n: big }).json();
	expect(t).toBe("int");

	// The SDK returns a native JS bigint (not a lossy number) and the value is
	// exactly preserved.
	const [out] = (await db.query("RETURN $n", { n: big })) as [unknown];
	expect(typeof out).toBe("bigint");
	expect(out).toBe(big);

	// .json() preserves it as a bigint too.
	const [outJson] = await db.query("RETURN $n", { n: big }).json();
	expect(typeof outJson).toBe("bigint");
	expect(outJson).toBe(big);

	// As a record-id key part it roundtrips exactly — no silent float coercion.
	const bigId = new RecordId("bignum", big);
	await db.query("CREATE $r SET v = 1", { r: bigId });
	const [rows] = (await db.query("SELECT * FROM $r", { r: bigId })) as [
		Array<{ id: RecordId }>,
	];
	expect(rows).toHaveLength(1);
	expect(rows[0].id).toBeInstanceOf(RecordId);
	expect(typeof rows[0].id.id).toBe("bigint");
	expect(rows[0].id.id).toBe(big);

	// And the same when reached by full-table scan rather than by bound id.
	const [all] = (await db.query("SELECT * FROM bignum")) as [
		Array<{ id: RecordId }>,
	];
	expect(all).toHaveLength(1);
	expect(all[0].id.id).toBe(big);

	await db.close();
});

test("ranges decode as Range with correct inclusive/exclusive/unbounded bounds", async () => {
	const { db } = await rootClient(server);

	// `..` is end-exclusive, `..=` end-inclusive; a missing side is unbounded.
	const cases: Array<{
		expr: string;
		begin: { cls: typeof BoundIncluded | typeof BoundExcluded; value: unknown } | null;
		end: { cls: typeof BoundIncluded | typeof BoundExcluded; value: unknown } | null;
		json: string;
	}> = [
		{
			expr: "1..5",
			begin: { cls: BoundIncluded, value: 1 },
			end: { cls: BoundExcluded, value: 5 },
			json: "1..5",
		},
		{
			expr: "1..=5",
			begin: { cls: BoundIncluded, value: 1 },
			end: { cls: BoundIncluded, value: 5 },
			json: "1..=5",
		},
		{
			expr: "'a'..'z'",
			begin: { cls: BoundIncluded, value: "a" },
			end: { cls: BoundExcluded, value: "z" },
			json: "a..z",
		},
		{
			expr: "1..",
			begin: { cls: BoundIncluded, value: 1 },
			end: null,
			json: "1..",
		},
		{
			expr: "..10",
			begin: null,
			end: { cls: BoundExcluded, value: 10 },
			json: "..10",
		},
	];

	for (const c of cases) {
		const [t] = await db.query<[string]>(`RETURN type::of(${c.expr})`).json();
		expect(t).toBe("range");

		const [out] = (await db.query(`RETURN ${c.expr}`)) as [Range<unknown, unknown>];
		expect(out).toBeInstanceOf(Range);
		if (c.begin === null) {
			// An unbounded side decodes as `undefined`, not a Bound.
			expect(out.begin).toBeUndefined();
		} else {
			expect(out.begin).toBeInstanceOf(c.begin.cls);
			expect((out.begin as BoundIncluded<unknown>).value).toEqual(c.begin.value);
		}
		if (c.end === null) {
			expect(out.end).toBeUndefined();
		} else {
			expect(out.end).toBeInstanceOf(c.end.cls);
			expect((out.end as BoundExcluded<unknown>).value).toEqual(c.end.value);
		}

		// .json() flattens the range to its SurrealQL string form.
		const [j] = await db.query(`RETURN ${c.expr}`).json();
		expect(j).toBe(c.json);
	}

	// A Range constructed on the SDK side roundtrips back as an equal Range —
	// and Range.equals works across the bundle's two class copies (unlike
	// RecordId.equals, which crashes on a cross-copy compare).
	const local = new Range(new BoundIncluded(1), new BoundExcluded(5));
	const [bt] = await db.query<[string]>("RETURN type::of($r)", { r: local }).json();
	expect(bt).toBe("range");
	const [bout] = (await db.query("RETURN $r", { r: local })) as [Range<number, number>];
	expect(bout).toBeInstanceOf(Range);
	expect(bout.equals(local)).toBe(true);
	expect((bout.begin as BoundIncluded<number>).value).toBe(1);
	expect((bout.end as BoundExcluded<number>).value).toBe(5);

	await db.close();
});

test("record-id ranges select the correct id subset; a bound RecordIdRange reports type record", async () => {
	const { db } = await rootClient(server);

	await db.query("CREATE |person:1..=10| SET n = 1 RETURN NONE");

	// An inline record-id range target selects exactly the inclusive subset.
	const [rows] = (await db.query("SELECT * FROM person:1..=5")) as [
		Array<{ id: RecordId }>,
	];
	expect(rows).toHaveLength(5);
	expect(rows.map((r) => String(r.id))).toEqual([
		"person:1",
		"person:2",
		"person:3",
		"person:4",
		"person:5",
	]);
	for (const row of rows) expect(row.id).toBeInstanceOf(RecordId);

	// A RecordIdRange bound from the SDK works as a scan target too. Surprising
	// but observed: type::of on a bound record-id range reports "record", NOT
	// "range" — a record-id range is a record target, not a plain range value.
	const rr = new RecordIdRange("person", new BoundIncluded(1), new BoundIncluded(3));
	const [rrt] = await db.query<[string]>("RETURN type::of($rr)", { rr }).json();
	expect(rrt).toBe("record");

	const [rrRows] = (await db.query("SELECT * FROM $rr", { rr })) as [
		Array<{ id: RecordId }>,
	];
	expect(rrRows.map((r) => String(r.id))).toEqual(["person:1", "person:2", "person:3"]);

	await db.close();
});

test("closures cannot cross the wire, but type::of still reports function", async () => {
	const { db } = await rootClient(server);

	// A closure has no public value representation: the whole response fails at
	// the core->public conversion, before any CBOR is produced.
	const bareErr = await rejects(db.query("RETURN |$x| $x + 1").collect());
	expect(bareErr.constructor.name).toBe("InternalError");
	expect(String(bareErr)).toMatch(/Closure values cannot be converted to public value/);

	// Nesting the closure inside an object does not help — the same conversion
	// failure takes down the entire response.
	const nestedErr = await rejects(db.query("RETURN { f: |$x| $x + 1 }").collect());
	expect(nestedErr.constructor.name).toBe("InternalError");
	expect(String(nestedErr)).toMatch(/Closure values cannot be converted to public value/);

	// The value still exists server-side: type::of names it without returning it.
	const [t] = await db.query<[string]>("RETURN type::of(|$x| $x)").json();
	expect(t).toBe("function");

	await db.close();
});

test("regex is never representable as a wire value; it exists only as an operand", async () => {
	const { db } = await rootClient(server);

	// A regex literal is only parsed as an operand of an expression. Any attempt
	// to surface one as a standalone value — returned, wrapped, or stored — is a
	// PARSE error, so a regex never reaches the CBOR encoder at all.
	for (const stmt of [
		"RETURN /abc/",
		"RETURN (/abc/)",
		"RETURN [/abc/]",
		"RETURN { r: /abc/ }",
		"CREATE re_probe:one SET r = /abc/",
	]) {
		const err = await rejects(db.query(stmt).collect());
		expect(err.constructor.name).toBe("ValidationError");
		expect(String(err)).toMatch(/Parse error/);
	}

	// As an operand it is fully live: type::of names it, and it matches.
	const [t] = await db.query<[string]>("RETURN type::of(/abc/)").json();
	expect(t).toBe("regex");
	const [matched] = await db.query<[boolean]>("RETURN 'abcd' = /abc/").json();
	expect(matched).toBe(true);

	await db.close();
});

test("<set> decodes as a native Set with dedup, distinct from a plain array", async () => {
	const { db } = await rootClient(server);

	// The server sees SurrealQL `set`; duplicates are collapsed.
	const [t] = await db.query<[string]>("RETURN type::of(<set>[1, 1, 2, 3])").json();
	expect(t).toBe("set");

	// The SDK decodes a set as a native JS Set (deduped), not an array — both
	// through the value path and through .json().
	const [out] = (await db.query("RETURN <set>[1, 1, 2, 3]")) as [unknown];
	expect(out).toBeInstanceOf(Set);
	expect(Array.isArray(out)).toBe(false);
	expect(Array.from(out as Set<number>)).toEqual([1, 2, 3]);

	const [outJson] = (await db.query("RETURN <set>[1, 1, 2, 3]").json()) as [unknown];
	expect(outJson).toBeInstanceOf(Set);
	expect(Array.from(outJson as Set<number>)).toEqual([1, 2, 3]);

	// A plain array literal keeps its duplicates and decodes as an Array.
	const [arr] = (await db.query("RETURN [1, 1, 2, 3]")) as [unknown];
	expect(Array.isArray(arr)).toBe(true);
	expect(arr).toEqual([1, 1, 2, 3]);

	// Stored in a record and read back, still a deduped Set.
	await db.query("CREATE set_probe:one SET s = <set>[1, 1, 2, 3]");
	const [rows] = (await db.query("SELECT * FROM set_probe:one")) as [
		Array<{ s: unknown }>,
	];
	expect(rows[0].s).toBeInstanceOf(Set);
	expect(Array.from(rows[0].s as Set<number>)).toEqual([1, 2, 3]);

	await db.close();
});

test("file references roundtrip as FileRef; the <file> cast and slashless literals are rejected", async () => {
	// The `f"bucket:/key"` literal is gated behind an experimental feature.
	const fileServer = await startServer({ args: ["--allow-experimental=files"] });
	try {
		const { db } = await rootClient(fileServer);

		// A file literal requires a `/`-prefixed key; the bucket and key survive.
		const [t] = await db.query<[string]>('RETURN type::of(f"docs:/readme.md")').json();
		expect(t).toBe("file");
		const [out] = (await db.query('RETURN f"docs:/readme.md"')) as [FileRef];
		expect(out).toBeInstanceOf(FileRef);
		expect(out.bucket).toBe("docs");
		expect(out.key).toBe("/readme.md");

		// .json() flattens a file to its `bucket:/key` string form.
		const [j] = await db.query('RETURN f"docs:/readme.md"').json();
		expect(j).toBe("docs:/readme.md");

		// A FileRef bound from the SDK roundtrips, and FileRef.equals works across
		// the bundle's class copies. A key given without a leading slash comes
		// back normalized WITH one.
		const bound = new FileRef("bucket", "key");
		const [bt] = await db.query<[string]>("RETURN type::of($f)", { f: bound }).json();
		expect(bt).toBe("file");
		const [bout] = (await db.query("RETURN $f", { f: bound })) as [FileRef];
		expect(bout).toBeInstanceOf(FileRef);
		expect(bout.bucket).toBe("bucket");
		expect(bout.key).toBe("/key");
		const slashed = new FileRef("bucket", "/key");
		expect(bout.equals(slashed)).toBe(true);

		// Stored in a record and read back, still a FileRef with parts intact.
		await db.query('CREATE file_probe:one SET f = f"docs:/readme.md"');
		const [rows] = (await db.query("SELECT * FROM file_probe:one")) as [
			Array<{ f: FileRef }>,
		];
		expect(rows[0].f).toBeInstanceOf(FileRef);
		expect(rows[0].f.bucket).toBe("docs");
		expect(rows[0].f.key).toBe("/readme.md");

		// A `<file>` cast from a string is NOT a valid path to a file value: it
		// fails at runtime regardless of whether the string carries a `/`.
		const castErr = await rejects(db.query('RETURN <file>"bucket:/key"').collect());
		expect(castErr.constructor.name).toBe("InternalError");
		expect(String(castErr)).toMatch(/Could not cast into `file`/);

		// A file literal whose key lacks a leading `/` is a parse error.
		const slashErr = await rejects(db.query('RETURN f"bucket:key"').collect());
		expect(slashErr.constructor.name).toBe("ValidationError");
		expect(String(slashErr)).toMatch(/Parse error/);

		await db.close();
	} finally {
		await fileServer.stop();
	}

	// Without the experimental feature the literal is rejected at parse time.
	const { db } = await rootClient(server);
	const gatedErr = await rejects(db.query('RETURN f"docs:/readme.md"').collect());
	expect(gatedErr.constructor.name).toBe("ValidationError");
	expect(String(gatedErr)).toMatch(/experimental files feature to be enabled/);
	await db.close();
});

test("the cast matrix decodes rich SurrealQL types as their SDK value classes", async () => {
	const { db } = await rootClient(server);

	// <datetime> -> DateTime
	const [dtT] = await db.query<[string]>("RETURN type::of(<datetime>'2026-07-17T12:34:56Z')").json();
	expect(dtT).toBe("datetime");
	const [dt] = (await db.query("RETURN <datetime>'2026-07-17T12:34:56Z'")) as [DateTime];
	expect(dt).toBeInstanceOf(DateTime);
	expect(dt.toISOString()).toBe("2026-07-17T12:34:56.000Z");

	// <duration> -> Duration
	const [durT] = await db.query<[string]>("RETURN type::of(<duration>'1h30m')").json();
	expect(durT).toBe("duration");
	const [dur] = (await db.query("RETURN <duration>'1h30m'")) as [Duration];
	expect(dur).toBeInstanceOf(Duration);
	expect(String(dur)).toBe("1h30m");

	// <uuid> -> Uuid
	const [uuidT] = await db
		.query<[string]>("RETURN type::of(<uuid>'018f2a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b')")
		.json();
	expect(uuidT).toBe("uuid");
	const [uuid] = (await db.query(
		"RETURN <uuid>'018f2a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b'",
	)) as [Uuid];
	expect(uuid).toBeInstanceOf(Uuid);
	expect(String(uuid)).toBe("018f2a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b");

	// <decimal> -> Decimal (full precision beyond f64)
	const [decT] = await db
		.query<[string]>("RETURN type::of(<decimal>'12345678901234567890.123456789')")
		.json();
	expect(decT).toBe("decimal");
	const [dec] = (await db.query(
		"RETURN <decimal>'12345678901234567890.123456789'",
	)) as [Decimal];
	expect(dec).toBeInstanceOf(Decimal);
	expect(String(dec)).toBe("12345678901234567890.123456789");

	// <record> -> RecordId
	const [recT] = await db.query<[string]>("RETURN type::of(<record>'thing:alpha')").json();
	expect(recT).toBe("record");
	const [rec] = (await db.query("RETURN <record>'thing:alpha'")) as [RecordId];
	expect(rec).toBeInstanceOf(RecordId);
	expect(String(rec)).toBe("thing:alpha");

	// <geometry> -> the concrete Geometry subclass
	const [geoT] = await db
		.query<[string]>("RETURN type::of(<geometry>{ type: 'Point', coordinates: [1, 2] })")
		.json();
	expect(geoT).toBe("geometry<point>");
	const [geo] = (await db.query(
		"RETURN <geometry>{ type: 'Point', coordinates: [1, 2] }",
	)) as [Geometry];
	expect(geo).toBeInstanceOf(GeometryPoint);
	expect(geo.toJSON()).toEqual({ type: "Point", coordinates: [1, 2] });

	// Numeric casts decode as plain JS numbers, distinguished only by type::of.
	// Surprising but observed: <number> of an integer yields `int`, not a
	// dedicated "number" type; <number> of a fractional value yields `float`.
	const [floatT] = await db.query<[string]>("RETURN type::of(<float>3)").json();
	expect(floatT).toBe("float");
	const [numIntT] = await db.query<[string]>("RETURN type::of(<number>3)").json();
	expect(numIntT).toBe("int");
	const [numFloatT] = await db.query<[string]>("RETURN type::of(<number>3.5)").json();
	expect(numFloatT).toBe("float");
	const [numVal] = (await db.query("RETURN <number>3.5")) as [unknown];
	expect(typeof numVal).toBe("number");
	expect(numVal).toBe(3.5);

	await db.close();
});
