/**
[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """
Setup for module_silo_rekey.surql: the old binary writes two module definitions,
one of which it keys under a name the current build does not derive from the
same value.

A module row lives at `!md{name}`, where `{name}` is not stored in the value but
derived from it, so which key a definition lands under is decided entirely by
the writing binary's derivation. Releases through 3.2 spell a silo package
`silo::<org>::<pkg><major.minor.patch>`; a named module is `mod::<name>` on every
release and on the current build alike, which is why `mod::modkey_plain` is
here. It is the control that has to come through the upgrade untouched and still
addressable, and it shares the band with the row that moves.

Both definitions name a silo package as their executable, so this script needs
neither a bucket nor the network: a silo executable is persisted with no fetch of
any kind on every release in the matrix, and the row that has to reach the new
binary is written by the same code path an operator's `DEFINE MODULE` takes.

`PERMISSIONS FULL` on both, deliberately. An expression-bearing permission would
also cross the stored-permission re-encoding, and a failure on the other side
would no longer say which of the two formats broke.

The version triple is spaced out because the integer lexer of the binaries that
run this script stops at a `.` followed by a digit, reading `1.2.3` as the start
of a float. The spacing is a parsing detail only; the stored name is formatted
from the three numbers and carries none of it."""
run = false
# The released spelling of a silo executable has no `::` before its version and
# no `UNSIGNED` keyword, neither of which the current parser accepts, so this
# script is only ever replayed against a spawned release.
legacy-syntax = true
*/
-- The subject. The COMMENT is carried across the upgrade verbatim, so it also
-- shows the value itself decoding intact rather than being reconstructed from
-- the key it happened to be filed under.
DEFINE MODULE silo::acme::modkey_widgets<1 . 2 . 3> COMMENT "legacy silo row" PERMISSIONS FULL;
-- The control. Its name is derived from the `name` field rather than from the
-- executable's coordinates, so every release and the current build agree on
-- where it belongs.
DEFINE MODULE mod::modkey_plain AS silo::acme::modkey_helpers<0 . 9 . 1> PERMISSIONS FULL;
