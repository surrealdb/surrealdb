/**
[test]
reason = "Repair script for field_union_element_definition.surql: removes the element definitions the old binary wrote beside a union kind with an `object` or literal array member."
run = false
*/
REMOVE FIELD v.* ON up_field_union_element_object;
REMOVE FIELD v.* ON up_field_union_element_literal;
