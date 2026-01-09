/**
[test]

[[test.results]]
value = "d'1979-09-11T00:00:00Z'"

[[test.results]]
value = "d'1979-09-11T00:00:00Z'"

[[test.results]]
error = "Could not cast into `datetime` using input `'1979-09_11'`"

*/

<datetime>'1979-09-11';
<datetime>'1979-09-11T00:00:00Z';
<datetime>'1979-09_11';