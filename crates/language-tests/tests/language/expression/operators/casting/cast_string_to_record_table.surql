/**
[test]

[[test.results]]
value = "a:1"

[[test.results]]
value = "a:1"

[[test.results]]
error = "Could not cast into `record<b>` using input `a:1`"

[[test.results]]
error = "Could not cast into `record<b>` using input `'a:1'`"

*/
<record<a>> a:1;
<record<a>> "a:1";
<record<b>> a:1;
<record<b>> "a:1";
