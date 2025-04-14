/**
[test]

[[test.results]]
value = "/a/"

[[test.results]]
value = "/a/"

[[test.results]]
error = "Expected a regex but cannot convert 1 into a regex"

*/

<regex> /a/ ;
<regex> "a";
<regex> 1;
