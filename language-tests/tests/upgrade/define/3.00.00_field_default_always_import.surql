/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, label: 'pre-upgrade' }]"

*/
DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD label ON doc TYPE string DEFAULT ALWAYS 'auto';
CREATE doc:1 SET label = 'pre-upgrade';
