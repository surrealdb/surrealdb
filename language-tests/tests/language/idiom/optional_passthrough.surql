/**
[test]
reason = "Test that optional chaining passes through all value types unchanged"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NULL"

[[test.results]]
value = "1"

[[test.results]]
value = "'a'"

[[test.results]]
value = "1s"

[[test.results]]
value = "d'1970-01-01T00:00:00Z'"

[[test.results]]
value = "u'0192fb97-e8ee-7683-8198-95710b103bd5'"

[[test.results]]
value = "[]"

[[test.results]]
value = "{  }"

[[test.results]]
value = "(89f, 90f)"

[[test.results]]
value = 'b"68686568656865"'

[[test.results]]
value = "person:aeon"

[[test.results]]
value = "{ type: 'Polygon', coordinates: [[[-111.069f, 45.0032f], [-104.0838f, 44.9893f], [-104.091f, 40.9974f], [-111.0672f, 40.9862f], [-111.069f, 45.0032f]]] }"
*/

none.?;
null.?;
1.?;
'a'.?;
1s.?;
time::EPOCH.?;
u'0192fb97-e8ee-7683-8198-95710b103bd5'.?;
[].?;
{}.?;
(89.0, 90.0).?;
<bytes>"hhehehe".?;
person:aeon.?;
{
	type: "Polygon",
	coordinates: [[
		[-111.0690, 45.0032],
		[-104.0838, 44.9893],
		[-104.0910, 40.9974],
		[-111.0672, 40.9862]
	]]
}.?;
