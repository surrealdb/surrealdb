/**
[test]
reason = """
Setup for module_definition_clauses.surql: the old binary defines a module the
way every 3.x release does, before a definition recorded a DIGEST, a SIGNER or
CAPABILITIES.

The executable is a file in a bucket that is never defined, so the definition is
written without fetching anything and the new binary resolves nothing when it
lists the module. A COMMENT rides along so the body can show the value decoding
intact rather than being reconstructed from its key. A second module,
`removable`, is there to be removed on the new binary."""
run = false
# The 3.3 spelling, which writes revision 2 with `UNSIGNED` recorded.
*/
DEFINE MODULE mod::legacy FROM f"mods:/legacy.surli" UNSIGNED COMMENT "written by 3.x" PERMISSIONS FULL;
DEFINE MODULE mod::removable FROM f"mods:/removable.surli" UNSIGNED;
