/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "The embedded-JS bridge `Value::from_js` converts a script's return value into a `Value` by native recursion. A script can build an arbitrarily deep value in a flat loop (`a = [a]`), which needs no JS-side recursion so the engine's own script-stack limit never engages; a return value of a few thousand levels then overflows the host stack inside `from_js` and aborts the process under `panic = 'abort'`, below the memory limit that would reject a wider value. The conversion is depth-bounded at the same cap as the rest of the engine, so a too-deep return value is rejected rather than aborting."

[[test.results]]
error = "Problem with embedded script function. Error converting from js 'js value' into type 'value': The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

*/

RETURN function() {
	let a = [];
	for (let i = 0; i < 10000; i++) { a = [a]; }
	return a;
};
