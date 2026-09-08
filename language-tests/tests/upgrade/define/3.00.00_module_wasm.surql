/**
[env]
imports = ["upgrade/define/3.00.00_module_wasm_import.surql"]

[env.capabilities]
allow-experimental = ["files", "surrealism"]

[test]
upgrade = true
# The import writes the pre-3.4 module spelling, which only a released binary
# parses, and the assertions below are how the current build renders and runs
# it. The lower bound is the fixture's, not the feature's: the module is built by
# the current toolchain, and 3.0.x cannot parse a component of that vintage
# ("failed to parse WebAssembly module"). These gates also keep the ordinary
# runner away, since it matches both against its own version.
importing-version = ">=3.1.0, <3.3.0-0"
version = ">=3.4.0-a"
reason = """
A Surrealism module is a wasm object living in a bucket outside the key-value
store, reached through a catalog definition. This pair is the only place the new
binary loads and executes wasm that an older binary wrote the definition for, and
calls into it against the very object that binary was using.

The values the old binary computed are stored alongside, so the two runs are
compared rather than merely both succeeding: a module that silently stopped
executing would have to return exactly what its predecessor did to pass."""

[[test.results]]
match = '''string::contains($result.modules['mod::demo'], 'demo.surli') AND string::contains($result.modules['mod::demo'], 'UNSIGNED') AND string::contains($result.buckets.mods, 'file:')'''

[[test.results]]
value = "true"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ drives: true, half: 5, id: wasm_marker:1 }]"

*/
-- 0: the definition the old binary wrote still renders, through the bucket it
-- named
INFO FOR DB;
-- 1, 2: the new binary loads the same wasm object out of that bucket and runs
-- it, so the module is executed across the upgrade rather than merely listed
RETURN mod::demo::can_drive(21);
RETURN mod::demo::safe_divide(10, 2);
-- 3: and it agrees with what the old binary computed from the same object
SELECT * FROM wasm_marker;
