const fs = require("node:fs");
const pg = require("pg");
const original = pg.Client.prototype.query;
const records = [];
pg.Client.prototype.query = function (...args) {
	const row = { sql: typeof args[0] === "string" ? args[0] : args[0]?.text };
	records.push(row);
	const error = (e) => {
		row.error = { code: e?.code, message: e?.message };
	};
	if (typeof args.at(-1) === "function") {
		const cb = args.at(-1);
		args[args.length - 1] = function (e, ...rest) {
			if (e) error(e);
			return cb(e, ...rest);
		};
	}
	try {
		const r = original.apply(this, args);
		if (r && typeof r.then === "function") r.then(() => {}, error);
		return r;
	} catch (e) {
		error(e);
		throw e;
	}
};
process.on("exit", () => {
	if (process.env.ORM_TRACE_PATH)
		fs.writeFileSync(
			process.env.ORM_TRACE_PATH,
			JSON.stringify(records, null, 2),
		);
});
