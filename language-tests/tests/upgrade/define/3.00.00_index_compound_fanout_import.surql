/**
[test]
reason = """
Setup for 3.00.00_index_compound_fanout.surql and its _current sibling: the old
binary writes a UNIQUE index over two columns that both fan out, so the new
binary reads a key set the old writer produced."""
run = false
*/
DEFINE TABLE tagged SCHEMALESS;
DEFINE INDEX tagged_pair ON tagged FIELDS tags.*, nums.* UNIQUE;
-- Two distinct elements in each column: the combination count is what the
-- writer's expansion decides, and a single-element array would hide it.
CREATE tagged:base SET tags = ['p', 'q'], nums = [1, 2];
CREATE tagged:other SET tags = ['z'], nums = [9];
