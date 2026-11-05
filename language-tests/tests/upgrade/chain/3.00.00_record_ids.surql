/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Carries one table of record ids — an integer key, a bare identifier key, and a
quoted string key containing whitespace — through every version in the chain.
The first version writes one record of each encoding, and every later version
adds its own into the same table before reading back everything any earlier
version wrote, addressing the original three by literal id.

A pairwise hop can only show that one release decodes what the release before it
encoded. This shows that a key written once keeps decoding to the same value
while every release in turn writes new keys of the same shape alongside it, so a
change to how a key is encoded cannot hide behind the fact that both ends of a
single hop agree."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_record_ids_setup.surql"
mutate = "upgrade/chain/3.00.00_record_ids_mutate.surql"

[[test.results]]
value = "[0]"

[[test.results]]
value = "['seed']"

[[test.results]]
value = "['seed record']"

[[test.results]]
match = '''
    count($result) == $hop + 1
    && $result[0] == chain_record_ids:0
    && type::record('chain_record_ids', $hop) in $result
'''

[[test.results]]
match = '''
    count($result) == $hop + 1
    && chain_record_ids:seed in $result
    && type::record('chain_record_ids', 'hop_' + <string> $hop) in $result
'''

[[test.results]]
match = '''
    count($result) == $hop + 1
    && chain_record_ids:`seed record` in $result
    && type::record('chain_record_ids', 'hop ' + <string> $hop) in $result
'''

[[test.results]]
match = '''
    count($result) == $hop + 1
    && count(array::distinct($result)) == $hop
'''

*/
-- The three records the first version wrote are still addressable by literal
-- id, and each key still decodes to the value it was written with: the integer
-- key is still an integer rather than a stringified one, and the quoted key
-- still holds its whitespace exactly.
SELECT VALUE record::id(id) FROM chain_record_ids:0;
SELECT VALUE record::id(id) FROM chain_record_ids:seed;
SELECT VALUE record::id(id) FROM chain_record_ids:`seed record`;
-- One record per encoding per version walked so far, the setup record included.
-- Ordering the integer keys puts key 0 first only while they still sort as
-- integers, and the current version's own key must be reconstructible from
-- $hop, which is what proves the encoding round-trips on this binary too.
SELECT VALUE id FROM chain_record_ids WHERE shape = 'int' ORDER BY id;
SELECT VALUE id FROM chain_record_ids WHERE shape = 'ident' ORDER BY id;
SELECT VALUE id FROM chain_record_ids WHERE shape = 'text' ORDER BY id;
-- Every release in turn wrote through this table: the setup version stamps two
-- of the rows, so the number of distinct writers is one fewer than the rows.
-- The ordering idiom has to be the projected field, which is what the oldest
-- binaries in the walk accept.
SELECT VALUE wrote_at FROM chain_record_ids WHERE shape = 'int' ORDER BY wrote_at;
