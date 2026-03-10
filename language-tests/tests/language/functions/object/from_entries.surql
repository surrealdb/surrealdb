/**
[test]

[[test.results]]
value = "{ a: 1, b: 2 }"

[[test.results]]
value = "{  }"

*/

object::from_entries([ [ 'a', 1 ], [ 'b', 2 ] ]);
object::from_entries([  ]);
