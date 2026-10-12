/**
[test]
reason = """
Setup for 3.00.00_id_components_float_in_int_position.surql: the old binary
writes a table with no definition for `id` whose record ids are arrays of a site
and a number, and one of the numbers is the float 5.0. A table whose id was
never typed keeps a float key as a float, so `counter:['a', 5.0]` is a
different record from `counter:['a', 5]`."""
run = false
*/
DEFINE TABLE counter SCHEMALESS;
-- An int at the number's position, and a float whose value is a whole number.
CREATE counter:['a', 4] SET hits = 1;
CREATE counter:['a', 5.0] SET hits = 2;
