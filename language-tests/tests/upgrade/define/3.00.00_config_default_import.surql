/**
[env]
remote-exclusive = true
# Root- and namespace-scoped state outlives a REMOVE NAMESPACE, so the ordinary
# runner has to be told to reset the shared datastore after this test. The
# upgrade runner ignores the flag — it gives every run its own store directory.
clean = true

[test]
reason = "Setup for 3.00.00_config_default.surql: the old binary replaces the root default config a new instance bootstraps with a namespace and database pair of its own, creates the pair it names, and leaves a record in that database for the fallback to be measured against."
run = false
*/
DEFINE NAMESPACE up_config_default_ns;
USE NAMESPACE up_config_default_ns;
DEFINE DATABASE up_config_default_db;
USE DATABASE up_config_default_db;
CREATE up_config_default_marker:1 SET note = 'written by the importing binary';
DEFINE CONFIG OVERWRITE DEFAULT NAMESPACE up_config_default_ns DATABASE up_config_default_db;
