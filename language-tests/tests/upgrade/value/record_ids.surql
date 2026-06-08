/**
[env]
imports = ["upgrade/value/record_ids_import.surql"]

[test]
upgrade = true
reason = "Common record-ID encodings (integer, identifier, quoted string with whitespace) round-trip through upgrade"

[[test.results]]
match = '''
    count($result) == 3
    && rid:1 in $result
    && rid:abc in $result
    && rid:⟨hello world⟩ in $result
'''

*/
SELECT VALUE id FROM rid;
