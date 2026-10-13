/**
[env]
imports = ["upgrade/define/3.00.00_module_silo_rekey_import.surql"]

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
upgrade = true
# The import writes the released silo spelling, which only a released binary
# parses, and re-keying it is something only this build's migration does. So
# the source must predate the spelling change, and the target must be at or
# after the prerelease that carries the migration — an unanchored ">=3.3.0"
# would also schedule the 3.2.4 -> 3.3.0-beta.3 adjacent pair, and beta.3
# already writes the new spelling without yet re-keying old rows, so that hop
# fails. These gates also keep the ordinary runner away, since it matches both
# against its own version.
importing-version = "<3.3.0-0"
version = ">=3.3.0-beta.4"
reason = """
A module definition's name is not stored in its value. `INFO`, `REMOVE`,
`ALTER`, `DEFINE ... OVERWRITE` and a call into one of a module's exports each
re-derive the name from the decoded definition and address the single key that
name spells. Releases through 3.2 derive a silo package as
`silo::<org>::<pkg><major.minor.patch>` where this build derives
`silo::<org>::<pkg>::<major.minor.patch>`, so a definition one of them wrote
sits under a key nothing addresses — and nothing reports it missing either. The
listing paths scan the band and recompute each name from the value, so the
module renders exactly as it should while every point operation on the name just
rendered answers that no such module exists.

Only the seed axis reaches this. An adjacent pair cannot: 3.3.0-beta.3 already
derives the name this build derives, so the hop that lands here has no writer
able to produce the drift, and the hop onto 3.3.0-beta.3 has no reader able to
resolve it. A cumulative walk cannot either — it arrives through every binary in
turn, where this state has to reach this build straight from the release that
wrote it.

What running it as an upgrade adds over unit coverage of the re-keying itself is
the two ends nothing else exercises. First, that a real released `DEFINE MODULE`
lands bytes this build can decode: a row that does not decode is skipped with a
warning, so the store would come up reporting success while leaving the module
permanently unreachable. Second, that a real `surreal start` over a persistent
store — version check, lease, ledger, stamp — selects the step and carries it to
completion.

`REMOVE` is the load-bearing assertion. It is the only statement here that
addresses the derived key directly, so it is the one that separates a store
where the row is reachable under the name `INFO` prints from a store where
`INFO` merely recomputes a name nothing can reach. The second listing has to
come after it: while a row under the written name and a row under the derived
name are both present they collapse into one object key, and it is removing the
addressable one that exposes anything left behind.

Read through the object form of `INFO FOR DB` rather than `... STRUCTURE`, which
resolves a silo executable's exports through the module runtime and would put a
package fetch in the middle of a storage assertion."""

[[test.results]]
value = '''
{
	"mod::modkey_plain": "DEFINE MODULE mod::modkey_plain FROM silo::acme::modkey_helpers::<0.9.1> UNSIGNED PERMISSIONS FULL",
	"silo::acme::modkey_widgets::<1.2.3>": "DEFINE MODULE silo::acme::modkey_widgets::<1.2.3> UNSIGNED COMMENT 'legacy silo row' PERMISSIONS FULL"
}
'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
{
	"mod::modkey_plain": "DEFINE MODULE mod::modkey_plain FROM silo::acme::modkey_helpers::<0.9.1> UNSIGNED PERMISSIONS FULL"
}
'''

*/
-- 0: both definitions the old binary wrote decode and render. The silo row
-- comes back under the name this build derives, carrying the old binary's
-- COMMENT verbatim, and the control row is beside it unchanged. `UNSIGNED` on
-- both is supplied by the field's own default: the stored value predates
-- signing, and a definition written before signing existed loads unsigned.
(INFO FOR DB).modules;
-- 1: the assertion the rest of the test exists for. `REMOVE` re-derives the
-- name and deletes the one key that name spells, so it returns NONE only when
-- the row is genuinely reachable there. A row still filed under the name its
-- writer derived makes this report the module as not existing — under exactly
-- the name the listing above just printed.
REMOVE MODULE silo::acme::modkey_widgets::<1.2.3>;
-- 2: and the band holds nothing but the control. Two rows for one module render
-- as a single object key while both are present, so a copy of the silo
-- definition left behind is only visible once the addressable one is gone.
(INFO FOR DB).modules;
