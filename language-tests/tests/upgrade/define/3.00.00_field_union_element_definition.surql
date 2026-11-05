/**
[env]
imports = ["upgrade/define/3.00.00_field_union_element_definition_import.surql"]

[test]
upgrade = true
reason = """
A field whose union kind pairs an `array<..>` member with an `object` or a literal array member
implies no definition for its elements, because `*` reaches the object's values and the literal
array's elements as surely as the array member's. A database written by a release that implied
one for such a field holds it as a definition in its own right, which nothing tells apart from
one an operator wrote, so it stays and goes on judging every element `*` reaches. The rows the
old binary wrote read back unchanged either way.

The body's writes are UPSERTs of fixed values, so the second run of the repair bracket leaves
the store as the first left it.
"""

[[test.repair]]
since = ">=3.0.0"
script = "upgrade/define/3.00.00_field_union_element_definition_repair.surql"
unrepaired = "broken"
reason = """
Releases up to 3.3 write a definition of `v.*` beside a field of kind `object | array<int>` or
`[int, string] | array<bool>`, with the array member's element kind, and apply it to the
object's values and the literal array's elements too, so they refuse values the field's kind
admits, such as `{ x: 'text' }` and `[1, 'a']`. A database they wrote keeps that definition on
an upgrade, because nothing tells it apart from one an operator wrote, and every target goes on
refusing those values until `REMOVE FIELD v.* ON <table>` removes it. The field's kind alone
judges its values after that."""

[[test.results]]
value = "[{ id: up_field_union_element_object:ints, v: [1, 2] }, { id: up_field_union_element_object:object, v: { x: 1 } }]"

[[test.results]]
value = "[{ id: up_field_union_element_literal:bools, v: [true, false] }]"

[[test.results]]
value = "[{ id: up_field_union_element_object:text, v: { x: 'text' } }]"

[[test.results]]
value = "[{ id: up_field_union_element_literal:pair, v: [1, 'a'] }]"

[[test.results]]
error = true
*/

-- The rows the old binary wrote read back unchanged.
SELECT * FROM up_field_union_element_object ORDER BY id;
SELECT * FROM up_field_union_element_literal ORDER BY id;
-- A value of the object member, and of the literal array member, is admitted.
UPSERT up_field_union_element_object:text SET v = { x: 'text' };
UPSERT up_field_union_element_literal:pair SET v = [1, 'a'];
-- A value of no member is refused.
UPSERT up_field_union_element_object:mixed SET v = [1, 'text'];
