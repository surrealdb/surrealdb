/**
[test]
reason = "Setup for field_element_union_with_any.surql: the old binary stores rows, then defines a field of kind `array | array<int>`, which writes a definition of its elements of kind `any | int` beside it. The field is defined last because a 3.3 release cannot read the table's definitions once that one is written."
run = false
*/
DEFINE TABLE up_field_element_any SCHEMALESS;
CREATE up_field_element_any:mixed SET v = ['a', 1];
CREATE up_field_element_any:ints SET v = [1, 2];
DEFINE FIELD v ON up_field_element_any TYPE array | array<int>;
