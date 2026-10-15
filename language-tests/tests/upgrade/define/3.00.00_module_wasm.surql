/**
[env]
imports = ["upgrade/define/3.00.00_module_wasm_import.surql"]

[env.capabilities]
allow-experimental = ["files", "surrealism"]

[test]
upgrade = true
# The fixture is built by the current toolchain and loaded by the source binary,
# so the source has to accept the ABI the current toolchain stamps. That ABI is
# 4, which no released binary accepts — the check is an equality, so a 3.2.x
# binary refuses the fixture outright ("Unsupported ABI version: expected 2, got
# 4"). The lower bound is therefore the first release that can load an ABI 4
# module, and until one ships no source version matches and this pair runs
# nowhere. These gates also keep the ordinary runner away, since it matches both
# against its own version.
importing-version = ">=3.4.0"
version = ">=3.4.0-a"
reason = """
A Surrealism module is a wasm object living in a bucket outside the key-value
store, reached through a catalog definition. This pair is the only place the new
binary loads and executes wasm that an older binary wrote the definition for, and
calls into it against the very object that binary was using.

The values the old binary computed are stored alongside, so the two runs are
compared rather than merely both succeeding: a module that silently stopped
executing would have to return exactly what its predecessor did to pass.

Both halves need one ABI. The object is what the two binaries share, so a run
needs a source binary that loads the ABI the current toolchain stamps; an object
built at the older ABI cannot stand in, because the reading binary refuses that
one just as firmly. Restore the coverage by lowering `importing-version` to the
first release that ships ABI 4, once there is one."""

[[test.results]]
match = '''string::contains($result.extensions['ext::demo'], 'demo.surli') AND string::contains($result.extensions['ext::demo'], 'UNSIGNED') AND string::contains($result.extensions['ext::demo'], 'DEFINE EXTENSION ext::demo FROM') AND $result.modules IS NONE AND string::contains($result.buckets.mods, 'file:')'''

[[test.results]]
value = "true"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ drives: true, half: 5, id: wasm_marker:1 }]"

[[test.results]]
value = "true"

*/
-- 0: the definition the old binary wrote still renders, through the bucket it
-- named, under `extensions` by `ext::` name
INFO FOR DB;
-- 1, 2: the new binary loads the same wasm object out of that bucket and runs
-- it, so the module is executed across the upgrade rather than merely listed
RETURN mod::demo::can_drive(21);
RETURN mod::demo::safe_divide(10, 2);
-- 3: and it agrees with what the old binary computed from the same object
SELECT * FROM wasm_marker;
-- 4: the `ext::` spelling calls the definition the old binary wrote as `mod::`
RETURN ext::demo::can_drive(21);
