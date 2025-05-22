/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

*/

let $error = "Found record: `likes:siy1jqk4jjv7njfrf1o0` which is a relation, but expected a  RELATION IN record<person> OUT record<person | thing>";
string::matches($error, /Found record: `likes:[a-zA-Z0-9]*` which is a relation, but expected a  RELATION IN record<person> OUT record<person \\| thing>/)
