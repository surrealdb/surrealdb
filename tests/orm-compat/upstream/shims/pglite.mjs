// Stand-in for @electric-sql/pglite used by the drizzle-kit pg tests. Each `new PGlite()` gets its
// own fresh database on the target named by ORM_COMPAT_TARGET (reference | surrealdb), so a test
// exercises the real server instead of an in-process engine. It implements only the PGlite surface
// the drizzle-kit tests and drizzle-orm/pglite call: query, exec, transaction, close.
import pg from "pg";

const target = process.env.ORM_COMPAT_TARGET;
const run = `${process.pid}_${Date.now().toString(36)}`;
let counter = 0;

const DATE_OIDS = [1082, 1114, 1184, 1115, 1182, 1185];
const raw = (v) => v;

function typesFor(parsers) {
	return {
		getTypeParser(oid, format) {
			if (parsers && parsers[oid]) return parsers[oid];
			if (DATE_OIDS.includes(oid)) return raw;
			return pg.types.getTypeParser(oid, format);
		},
	};
}

async function provision() {
	const n = `kit_${run}_${counter++}`;
	if (target === "reference") {
		const admin = new pg.Client({ host: "127.0.0.1", port: 15433, user: "root", password: "root", database: "postgres" });
		await admin.connect();
		// Tests run one at a time, so every earlier instance is finished; dropping its database also releases the roles it owned.
		const old = await admin.query(`SELECT datname FROM pg_database WHERE datname LIKE 'kit\\_%'`);
		for (const row of old.rows) await admin.query(`DROP DATABASE IF EXISTS "${row.datname}" WITH (FORCE)`);
		await admin.query(`CREATE DATABASE ${n}`);
		// A PGlite instance owns its roles; the shared reference cluster must look the same per instance.
		await admin.query(`DO $$ DECLARE r record; BEGIN
			FOR r IN SELECT rolname FROM pg_roles WHERE rolname NOT LIKE 'pg\\_%' AND rolname NOT IN ('root', 'postgres') LOOP
				EXECUTE format('DROP ROLE %I', r.rolname); END LOOP;
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN CREATE ROLE postgres SUPERUSER; END IF;
		END $$`);
		await admin.end();
		return { config: { host: "127.0.0.1", port: 15433, user: "root", password: "root", database: n }, name: n };
	}
	return { config: { host: "127.0.0.1", port: 15432, user: "root", database: `orm/${n}`, options: "-c dialect=pgsql" }, name: n };
}

export class PGlite {
	constructor() {
		this.closed = false;
		this.ready = false;
		this.waitReady = (async () => {
			const { config, name } = await provision();
			this.name = name;
			this.client = new pg.Client({ ...config, connectionTimeoutMillis: 5000 });
			this.client.on("error", () => {});
			await this.client.connect();
			this.ready = true;
		})();
		this.waitReady.catch(() => {});
	}
	static async create() {
		const db = new PGlite();
		await db.waitReady;
		return db;
	}
	async query(text, params, options = {}) {
		await this.waitReady;
		const res = await this.client.query({
			text,
			values: params ?? [],
			rowMode: options.rowMode === "array" ? "array" : undefined,
			types: typesFor(options.parsers),
		});
		const last = Array.isArray(res) ? res[res.length - 1] : res;
		return {
			rows: last.rows ?? [],
			fields: (last.fields ?? []).map((f) => ({ name: f.name, dataTypeID: f.dataTypeID })),
			affectedRows: last.rowCount ?? 0,
		};
	}
	async exec(text) {
		await this.waitReady;
		const res = await this.client.query({ text, types: typesFor() });
		return (Array.isArray(res) ? res : [res]).map((r) => ({ rows: r.rows ?? [], fields: [], affectedRows: r.rowCount ?? 0 }));
	}
	async transaction(fn) {
		await this.query("BEGIN");
		const tx = { query: (t, p, o) => this.query(t, p, o), exec: (t) => this.exec(t), rollback: async () => { tx.rolledBack = true; } };
		try {
			const out = await fn(tx);
			await this.query(tx.rolledBack ? "ROLLBACK" : "COMMIT");
			return out;
		} catch (e) {
			await this.query("ROLLBACK").catch(() => {});
			throw e;
		}
	}
	async close() {
		if (this.closed) return;
		this.closed = true;
		try {
			await this.waitReady;
			await this.client.end();
		} catch {}
	}
}
export default { PGlite };
