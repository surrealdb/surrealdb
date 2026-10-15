/**
[env]
imports = ["upgrade/define/3.00.00_module_definition_clauses_import.surql"]

[env.capabilities]
allow-experimental = ["files", "surrealism"]

[test]
upgrade = true
# Releases from 3.3 write revision 2 with this spelling; earlier releases are
# covered by the `_rev1` pair. Only a reader on this line decodes revision 3. These gates also keep the ordinary runner away, since it matches
# both against its own version.
importing-version = ">=3.3.0, <3.4.0-0"
version = ">=3.4.0-a"
reason = """
A module definition written by any 3.x release predates the DIGEST, SIGNER and
CAPABILITIES clauses that `StoredModuleDefinition` revision 3 records. Such a
definition must stay in the catalog, readable and removable, but it must not
run: its archive was built for an ABI this build has no loader for, and the
definition grants no capabilities. The operator reinstalls it.

This pair is what proves that against bytes a released binary wrote. The frozen
fixtures in `kvs/compat/module_definition.rs` prove the revision-2 shape
decodes; they cannot prove that a real 3.x `DEFINE MODULE` lands that shape, or
that the listing, the structure, the call path and an overwrite all treat the
decoded row as a definition to reinstall. A row that failed to decode would be
skipped with a warning, so the module would vanish while the store reported
success.

Read through `INFO FOR DB` as well as `... STRUCTURE`: the first is the
rendering an export writes, the second carries the warning."""

[[test.results]]
value = '''{ "ext::legacy": "DEFINE EXTENSION ext::legacy FROM f\"mods:/legacy.surli\" UNSIGNED COMMENT 'written by 3.x' PERMISSIONS FULL", "ext::removable": 'DEFINE EXTENSION ext::removable FROM f"mods:/removable.surli" UNSIGNED PERMISSIONS FULL' }'''

[[test.results]]
value = "[{ comment: 'written by 3.x', executable: { bucket: 'mods', key: '/legacy.surli', type: 'surrealism' }, name: 'legacy', permissions: true, unsigned: true, warnings: ['This extension was defined before extensions recorded a DIGEST, a SIGNER and CAPABILITIES, so it does not run. Rebuild it for this release and define it again with DEFINE EXTENSION OVERWRITE.'] }, { executable: { bucket: 'mods', key: '/removable.surli', type: 'surrealism' }, name: 'removable', permissions: true, unsigned: true, warnings: ['This extension was defined before extensions recorded a DIGEST, a SIGNER and CAPABILITIES, so it does not run. Rebuild it for this release and define it again with DEFINE EXTENSION OVERWRITE.'] }]"

[[test.results]]
error = "The extension `ext::legacy` cannot run: This extension was defined before extensions recorded a DIGEST, a SIGNER and CAPABILITIES, so it does not run. Rebuild it for this release and define it again with DEFINE EXTENSION OVERWRITE."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ capabilities: { functions: false, memory: NONE, net: false, queries: false, scripting: false, strict_timeout: true, timeout: NONE, write: false }, comment: 'reinstalled', digest: 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d', executable: { bucket: 'mods', key: '/legacy.surli', type: 'surrealism' }, name: 'legacy', permissions: true, unsigned: true }, { executable: { bucket: 'mods', key: '/removable.surli', type: 'surrealism' }, name: 'removable', permissions: true, unsigned: true, warnings: ['This extension was defined before extensions recorded a DIGEST, a SIGNER and CAPABILITIES, so it does not run. Rebuild it for this release and define it again with DEFINE EXTENSION OVERWRITE.'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['ext::legacy']"


*/
-- 0: the old definition renders with what it recorded: no DIGEST and no
-- CAPABILITIES, and UNSIGNED, which every 3.x definition is.
(INFO FOR DB).extensions;
-- 1: its structure says it must be reinstalled.
(INFO FOR DB STRUCTURE).extensions;
-- 2: and it does not run. The refusal comes before the archive is fetched, so
-- the missing bucket plays no part in it.
RETURN ext::legacy::run();
-- 3: reinstalling it with the clauses revision 3 records clears the warning.
DEFINE EXTENSION OVERWRITE ext::legacy FROM f"mods:/legacy.surli"
    DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d"
    UNSIGNED
    CAPABILITIES {}
    COMMENT "reinstalled";
-- 4
(INFO FOR DB STRUCTURE).extensions;
-- 5: an old definition is removable as it is, without being reinstalled.
REMOVE EXTENSION ext::removable;
-- 6
(INFO FOR DB).extensions.keys();
