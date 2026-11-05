// Jest reporter that appends one JSON line per finished test. Set ORM_COMPAT_JSONL.
const fs = require("fs");
const path = require("path");

class JsonlReporter {
	constructor(globalConfig) {
		this.root = process.env.ORM_COMPAT_ROOT || globalConfig.rootDir;
	}
	onTestCaseResult(test, r) {
		const file = path.relative(this.root, test.path || "");
		const id = `${file}::${r.fullName}`;
		let row;
		if (r.status === "passed") row = { id, status: "pass", ms: r.duration };
		else if (r.status === "failed") {
			const msg = (r.failureMessages || [""])[0] || "";
			const m = /\bcode: '([0-9A-Z]{5})'/.exec(msg) || /\bSQLSTATE[ =:]*([0-9A-Z]{5})/.exec(msg);
			row = { id, status: "fail", message: msg.replace(/\u001b\[[0-9;]*m/g, "").slice(0, 2000), code: m ? m[1] : null, ms: r.duration };
		} else row = { id, status: "skip" };
		fs.appendFileSync(process.env.ORM_COMPAT_JSONL, JSON.stringify(row) + "\n");
	}
	onTestResult(test, result) {
		if (result.testExecError || (result.failureMessage && result.testResults.length === 0)) {
			const msg = String((result.testExecError && result.testExecError.message) || result.failureMessage || "suite failed to run").replace(/\u001b\[[0-9;]*m/g, "");
			fs.appendFileSync(process.env.ORM_COMPAT_JSONL, JSON.stringify({ id: `${path.relative(this.root, test.path)}::[suite failed to run]`, status: "fail", message: msg.slice(0, 2000), code: null }) + "\n");
		}
	}
}
module.exports = JsonlReporter;
