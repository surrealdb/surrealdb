/**
[test]
reason = "Setup for field_union_element_definition.surql: the old binary defines fields whose union kinds pair an `array<..>` member with an `object` or literal array member, which writes a definition for their elements beside each, and stores rows that definition admits."
run = false
*/
DEFINE TABLE up_field_union_element_object SCHEMALESS;
DEFINE FIELD v ON up_field_union_element_object TYPE object | array<int>;
CREATE up_field_union_element_object:object SET v = { x: 1 };
CREATE up_field_union_element_object:ints SET v = [1, 2];

DEFINE TABLE up_field_union_element_literal SCHEMAFULL;
DEFINE FIELD v ON up_field_union_element_literal TYPE [int, string] | array<bool>;
CREATE up_field_union_element_literal:bools SET v = [true, false];
