// Vitest reporter that appends one JSON line per finished test. Set ORM_COMPAT_JSONL.
import fs from "node:fs";
import path from "node:path";

function sqlstate(err) {
	if (err && typeof err.code === "string" && /^[0-9A-Z]{5}$/.test(err.code)) return err.code;
	const cause = err && err.cause;
	if (cause && typeof cause.code === "string" && /^[0-9A-Z]{5}$/.test(cause.code)) return cause.code;
	return null;
}

export default class JsonlReporter {
	onInit(ctx) {
		this.root = process.env.ORM_COMPAT_ROOT || ctx.config.root;
	}
	write(row) {
		fs.appendFileSync(process.env.ORM_COMPAT_JSONL, JSON.stringify(row) + "\n");
	}
	record(test) {
		const result = test.result?.();
		const state = result?.state ?? test.result?.state;
		const names = [];
		let p = test;
		while (p) {
			if (p.name) names.unshift(p.name);
			p = p.parent;
		}
		const file = test.module?.moduleId ?? test.file?.filepath ?? "";
		const id = `${path.relative(this.root, file)}::${names.join(" > ")}`;
		if (state === "passed") this.write({ id, status: "pass" });
		else if (state === "failed") {
			const err = result?.errors?.[0] ?? {};
			this.write({ id, status: "fail", message: String(err.message ?? "").slice(0, 2000), code: sqlstate(err) });
		} else this.write({ id, status: "skip" });
	}
	onTestCaseResult(test) {
		this.record(test);
	}
}
