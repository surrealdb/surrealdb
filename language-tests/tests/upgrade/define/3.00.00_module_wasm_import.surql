/**
[env.capabilities]
allow-experimental = ["files", "surrealism"]

[test]
reason = """
Setup for module_wasm.surql: the old binary defines a file-backed bucket over
the directory holding the built Surrealism module, defines a module from it, and
calls into it. Everything after this runs on the new binary against the same
store.

`$module_backend` is bound by the runner, which builds the module once and puts its
directory on the spawned binary's bucket allowlist. `BACKEND` takes an
expression, so the path never has to be written into the file."""
run = false
legacy-syntax = true
*/
DEFINE BUCKET mods BACKEND $module_backend;
DEFINE MODULE mod::demo AS f"mods:/demo.surli";
CREATE wasm_marker:1 SET drives = mod::demo::can_drive(21), half = mod::demo::safe_divide(10, 2);
