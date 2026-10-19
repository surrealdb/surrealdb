/**
[test]
reason = "A view's rows and groups are built from the values of its source, and a removal step clears values from the source's rows without rebuilding the view: REMOVE FIELD, and a stored field made COMPUTED, are refused on an enforced table with rows while a view reads the field, until the view is removed. A view that does not read the field is not affected, and the field is removed as it is for a table with no view. The view and its rows are as they were after the refusal."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change field `a` of table `src`: the view `tot` reads it: remove the view first"

[[test.results]]
error = "Cannot change the value the computed field `a` of `src` derives: the aggregated view `tot` depends on it, and `src` holds records the view has already grouped by the value derived before. Remove `tot`, change the field, then define `tot` again"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'x', id: tot:['x'], s: 1f }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE int;
DEFINE FIELD g ON src TYPE string;
DEFINE FIELD n ON src TYPE option<string>;
CREATE src:1 SET a = 1, g = 'x', n = 'unread' RETURN NONE;
DEFINE TABLE tot AS SELECT g, math::sum(a) AS s FROM src GROUP BY g;
-- Both removals clear values the view was built from. An aggregated view's own check of a changed
-- derivation judges the second before the rules of the table's migrations are reached.
REMOVE FIELD a ON src;
DEFINE FIELD OVERWRITE a ON src COMPUTED 1;
-- A field the view does not read is removed as it is without a view.
REMOVE FIELD n ON src;
SELECT * FROM tot;
-- The way the refusal asks for.
REMOVE TABLE tot;
REMOVE FIELD a ON src;
