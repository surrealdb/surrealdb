/**
[test]

[[test.results]]
value = "a:1"

[[test.results]]
value = "a:1"

[[test.results]]
error = "Expected `record<b>` but found a `a:1`"

[[test.results]]
error = "Expected `record<b>` but found a `'a:1'`"

*/
<record<a>> a:1;
<record<a>> "a:1";
<record<b>> a:1;
<record<b>> "a:1";
