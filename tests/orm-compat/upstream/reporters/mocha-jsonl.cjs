// Mocha reporter that appends one JSON line per finished test and flushes immediately, so a run
// killed at its wall-clock cap still leaves usable results. Set ORM_COMPAT_JSONL to the output path.
const fs = require("fs");
const path = require("path");
const { Runner } = require("mocha");
const E = Runner.constants;

function sqlstate(err) {
	if (err && typeof err.code === "string" && /^[0-9A-Z]{5}$/.test(err.code)) return err.code;
	const m = /\bSQLSTATE[ =:]*([0-9A-Z]{5})\b/.exec((err && err.message) || "");
	return m ? m[1] : null;
}

module.exports = function (runner) {
	const out = process.env.ORM_COMPAT_JSONL;
	const root = process.env.ORM_COMPAT_ROOT || process.cwd();
	const write = (row) => fs.appendFileSync(out, JSON.stringify(row) + "\n");
	const id = (t) => `${path.relative(root, t.file || "")}::${t.fullTitle()}`;
	runner.on(E.EVENT_TEST_PASS, (t) => write({ id: id(t), status: "pass", ms: t.duration }));
	runner.on(E.EVENT_TEST_PENDING, (t) => write({ id: id(t), status: "skip" }));
	runner.on(E.EVENT_TEST_FAIL, (t, err) =>
		write({
			id: id(t) + (t.type === "hook" ? " [hook]" : ""),
			status: "fail",
			message: String((err && err.message) || err).slice(0, 2000),
			code: sqlstate(err),
			query: err && typeof err.query === "string" ? err.query.slice(0, 400) : undefined,
			ms: t.duration,
		}),
	);
};
