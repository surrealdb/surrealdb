/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: product:test, images: ['test.png'] }]"

*/
DEFINE FIELD images ON product
	TYPE array
	ASSERT array::len($value) > 0;
DEFINE FIELD images.* ON product TYPE string
	VALUE string::trim($input)
	ASSERT $input AND string::len($value) > 0;
CREATE product:test SET images = [' test.png '];
