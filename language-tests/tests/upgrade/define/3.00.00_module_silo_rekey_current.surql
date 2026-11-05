/**
[env]
imports = ["upgrade/define/3.00.00_module_silo_rekey_import.surql"]

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
upgrade = true
# The import writes the released silo spelling, which only a released binary
# parses, so the source must predate the spelling change. The target is the
# current checkout, the only build that renders `DEFINE EXTENSION`; the 3.3 line
# is covered by 3.00.00_module_silo_rekey.surql. These gates also keep the
# ordinary runner away, since it matches both against its own version.
importing-version = "<3.3.0-0"
version = ">=3.4.0-a"
reason = """
A silo module definition a release through 3.2 wrote reads back on this build
in the EXTENSION spelling, and `REMOVE` reaches it under the name that spelling
prints. Only the current checkout renders `DEFINE EXTENSION`, so this case is
gated to it. The sibling `3.00.00_module_silo_rekey.surql` carries the same
assertions in the spelling the 3.3 line renders onto 3.3.0, and its `reason`
explains why the drift exists and why `REMOVE` is the load-bearing
statement."""

[[test.results]]
value = '''
{
	"ext::modkey_plain": "DEFINE EXTENSION ext::modkey_plain FROM silo::acme::modkey_helpers<0.9.1> UNSIGNED PERMISSIONS FULL",
	"silo::acme::modkey_widgets<1.2.3>": "DEFINE EXTENSION silo::acme::modkey_widgets<1.2.3> UNSIGNED COMMENT 'legacy silo row' PERMISSIONS FULL"
}
'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
{
	"ext::modkey_plain": "DEFINE EXTENSION ext::modkey_plain FROM silo::acme::modkey_helpers<0.9.1> UNSIGNED PERMISSIONS FULL"
}
'''

*/
-- 0: both definitions the old binary wrote decode and render, in the
-- EXTENSION spelling this build writes. The silo row comes back under the name
-- this build derives, carrying the old binary's COMMENT verbatim, and the
-- control row is beside it unchanged. The catalog loads an unnamed silo
-- definition, which no ordinary statement of this build creates. `UNSIGNED` on
-- both is supplied by the field's own default: the stored value predates
-- signing, and a definition written before signing existed loads unsigned.
(INFO FOR DB).extensions;
-- 1: the assertion the rest of the test exists for. `REMOVE` re-derives the
-- name and deletes the one key that name spells, so it returns NONE only when
-- the row is genuinely reachable there. A row still filed under the name its
-- writer derived makes this report the module as not existing — under exactly
-- the name the listing above just printed.
REMOVE EXTENSION silo::acme::modkey_widgets<1.2.3>;
-- 2: and the band holds nothing but the control. Two rows for one module render
-- as a single object key while both are present, so a copy of the silo
-- definition left behind is only visible once the addressable one is gone.
(INFO FOR DB).extensions;
