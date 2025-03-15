/**
[test]

[[test.results]]
value = "a:1"

[[test.results]]
value = "a:1"

[[test.results]]
error = "Expected a record<b> but cannot convert a:1 into a record<b>"

[[test.results]]
error = "Expected a record<b> but cannot convert 'a:1' into a record<b>"

*/
<record<a>> a:1;
<record<a>> "a:1";
<record<b>> a:1;
<record<b>> "a:1";
