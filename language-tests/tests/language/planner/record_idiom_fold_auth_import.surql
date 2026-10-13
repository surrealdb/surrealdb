/**
[test]
reason = "Setup for record_idiom_fold_auth_new_executor: access, tables, index, data"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:alice }]"

[[test.results]]
value = "[{ cutoff: d'2026-01-11T00:00:00Z', id: cfg:one }]"

[[test.results]]
value = "[{ due: d'2026-01-01T00:00:00Z', id: item:a }]"

[[test.results]]
value = "[{ due: d'2026-02-01T00:00:00Z', id: item:b }]"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD DURATION FOR SESSION 12h;
DEFINE TABLE cfg PERMISSIONS FOR select FULL;
DEFINE TABLE item PERMISSIONS FOR select FULL;
DEFINE INDEX idx_due ON TABLE item COLUMNS due;
CREATE user:alice;
CREATE cfg:one SET cutoff = d'2026-01-11T00:00:00Z';
CREATE item:a SET due = d'2026-01-01T00:00:00Z';
CREATE item:b SET due = d'2026-02-01T00:00:00Z';
