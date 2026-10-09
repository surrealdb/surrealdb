/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
DEFINE TABLE person;
# `person` is empty, so SELECT never evaluates the projection — `ml::foo` is
# never called and the "Machine learning computation is not enabled." error
# from the `cfg(not(feature = "ml"))` stub in
# exec/vm/calls/model.rs is never raised.
# `RETURN ml::foo<1.0.0>()` would evaluate unconditionally and hit it.
SELECT ml::foo<1.0.0>() FROM person;
