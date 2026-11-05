/**
[test]
reason = "Chain setup for relation.surql: the first walked version defines the RELATION table, the hub and seed vertices, and the one seed edge every later version has to keep traversable. Its edge carries hop 0, which the body filters on."
run = false
*/
DEFINE TABLE chain_rel_person SCHEMALESS;
DEFINE TABLE chain_rel_knows TYPE RELATION FROM chain_rel_person TO chain_rel_person SCHEMALESS;
CREATE chain_rel_person:alice SET hop = 0, wrote_at = $version;
CREATE chain_rel_person:bob SET hop = 0, wrote_at = $version;
RELATE chain_rel_person:alice -> chain_rel_knows -> chain_rel_person:bob SET hop = 0, wrote_at = $version;
