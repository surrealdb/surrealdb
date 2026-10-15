/**
[test]
reason = """
Setup for module_definition_clauses_rev1.surql: the old binary defines a module the
way every 3.x release does, before a definition recorded a DIGEST, a SIGNER or
CAPABILITIES.

The executable is a file in a bucket that is never defined, so the definition is
written without fetching anything and the new binary resolves nothing when it
lists the module. A COMMENT rides along so the body can show the value decoding
intact rather than being reconstructed from its key. A second module,
`removable`, is there to be removed on the new binary."""
run = false
# Released binaries spell the executable after `AS` and have no `UNSIGNED`
# keyword; the current parser accepts neither, so this script only ever runs on
# a spawned release.
legacy-syntax = true
*/
DEFINE MODULE mod::legacy AS f"mods:/legacy.surli" COMMENT "written by 3.x" PERMISSIONS FULL;
DEFINE MODULE mod::removable AS f"mods:/removable.surli";
