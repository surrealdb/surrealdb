/**
[test]
reason = """
Setup for 3.00.00_index_value_shapes.surql and its _current sibling: the old
binary builds two plain B-tree indexes over a column with no declared kind and
records no value shapes for either. One index stores an array in its column,
listed once per element; the other stores only datetimes."""
run = false
*/
DEFINE TABLE shape_mixed SCHEMALESS;
DEFINE INDEX shape_mixed_time ON shape_mixed FIELDS time;
CREATE shape_mixed:1 SET time = d'2024-01-01T00:00:00Z';
CREATE shape_mixed:2 SET time = d'2024-01-02T00:00:00Z';
CREATE shape_mixed:3 SET time = d'2024-01-03T00:00:00Z';
-- An array sorts after every datetime, but its one entry sits at its element,
-- below every other entry.
CREATE shape_mixed:arr SET time = [d'2023-01-01T00:00:00Z'];

DEFINE TABLE shape_plain SCHEMALESS;
DEFINE INDEX shape_plain_time ON shape_plain FIELDS time;
CREATE shape_plain:1 SET time = d'2024-01-01T00:00:00Z';
CREATE shape_plain:2 SET time = d'2024-01-02T00:00:00Z';
CREATE shape_plain:3 SET time = d'2024-01-03T00:00:00Z';
