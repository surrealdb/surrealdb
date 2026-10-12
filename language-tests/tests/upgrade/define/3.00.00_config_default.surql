/**
[env]
remote-exclusive = true
# Root- and namespace-scoped state outlives a REMOVE NAMESPACE, so the ordinary
# runner has to be told to reset the shared datastore after this test. The
# upgrade runner ignores the flag — it gives every run its own store directory.
clean = true
imports = ["upgrade/define/3.00.00_config_default_import.surql"]

[test]
upgrade = true
reason = """
DEFINE CONFIG DEFAULT stores a root-scoped StoredConfigDefinition::Default —
the namespace and database a session with no selection of its own falls back to
— under a single root config key, which is the only config that lives outside
a database.

The importing binary overwrites the pair a new instance bootstraps with names
of its own. A store is initialised only while it is new, so the pair read back
after the upgrade can only be the one the older binary wrote, never the target
binary's own initialisation showing through.

INFO FOR ROOT reports `defaults` as the structured form of the decoded catalog
value rather than as rendered DDL, so the assertion lands on the decode itself.
USE DEFAULT then spends that value instead of only reading it. The runner
selects a namespace and database on the session before the body runs, so
landing on the imported pair — and reading back a record only the importing
binary ever wrote there — is what shows the fallback resolves through the stored
config rather than through the session.

The overwrite that follows proves the newer binary writes back over the key the
older one wrote: the namespace-only shape it stores has to read back with no
`database` entry at all, since the field is absent rather than empty when unset,
and the USE DEFAULT after it moves the session's namespace alone, leaving the
database the previous USE DEFAULT selected.
"""

[[test.results]]
match = "$result.defaults == { namespace: 'up_config_default_ns', database: 'up_config_default_db' }"

[[test.results]]
value = "{ database: 'up_config_default_db', namespace: 'up_config_default_ns' }"

[[test.results]]
value = "['written by the importing binary']"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.defaults == { namespace: 'up_config_default_alt_ns' }"

[[test.results]]
value = "{ database: 'up_config_default_db', namespace: 'up_config_default_alt_ns' }"

*/
INFO FOR ROOT;
-- The session already names a namespace and a database, so a fallback that
-- landed anywhere but the imported pair could not read this record.
USE DEFAULT;
SELECT VALUE note FROM up_config_default_marker;
-- The stored default is not merely readable: the new binary replaces it in
-- place, and a config naming only a namespace stores no database at all.
DEFINE CONFIG OVERWRITE DEFAULT NAMESPACE up_config_default_alt_ns;
INFO FOR ROOT;
USE DEFAULT;
