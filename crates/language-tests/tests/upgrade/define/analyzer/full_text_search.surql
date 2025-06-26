/**
[env]
imports = ["upgrade/define/analyzer/full_text_search_import.surql"]

[test]
upgrade = true

[[test.results]]
value = "[{ name: '<em>Tobie</em>' }]"
*/

SELECT search::highlight('<em>','</em>', 1) AS name FROM account WHERE name @1@ 'Tobie';
