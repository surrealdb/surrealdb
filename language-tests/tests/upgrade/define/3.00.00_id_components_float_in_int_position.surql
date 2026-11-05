/**
[env]
imports = ["upgrade/define/3.00.00_id_components_float_in_int_position_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
An int component holds only ints. A float key that equals a whole number is
stored under the float's own bytes, which a range over the int keys of the same
position would never reach, so naming the position `n: int` is refused and the
error says the component is a float, rather than reading the key as the int it
resembles. The older writer's rows are as it left them, and a plain upgrade needs
no action: only the opt-in definition is refused.

The syntax is this checkout's. No released binary parses a named record id, so
the body runs only where the upgrade target is this checkout: the seed axis,
each old release straight to HEAD, and the final adjacent hop. The
release-to-release hops of the pair axis and the cumulative walk cannot reach
the construct until a release ships the syntax."""

[[test.results]]
error = "Cannot give `counter` the record id [site: string, n: int]: the existing record counter:['a', 5f] does not match it (`n` is a float; an int component holds only ints). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{  }"

[[test.results]]
value = "[1, 2]"

*/
-- A float stands where the record id needs an int.
DEFINE FIELD OVERWRITE id ON counter TYPE [site: string, n: int];
-- The refused statement defined nothing.
RETURN (INFO FOR TABLE counter).fields;
-- Both rows the old binary wrote are still there.
SELECT VALUE hits FROM counter ORDER BY hits;
