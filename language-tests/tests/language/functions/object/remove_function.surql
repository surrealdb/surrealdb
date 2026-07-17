/**
[test]
reason = "Tests object::remove function-call form: single key, array of keys, missing keys as no-op, and non-string key rejection (existing remove.surql only covers method form)"

[[test.results]]
value = "{ b: 2 }"

[[test.results]]
value = "{ b: 2 }"

[[test.results]]
value = "{ a: 1 }"

[[test.results]]
value = "{ a: 1 }"

[[test.results]]
value = "{  }"

[[test.results]]
error = "Incorrect arguments for function object::remove(). '1' cannot be used as a key. Please use a string instead."

[[test.results]]
error = "Incorrect arguments for function object::remove(). '123' cannot be used as a key. Please use a string instead."
*/

object::remove({ a: 1, b: 2 }, 'a');
object::remove({ a: 1, b: 2, c: 3 }, ['a', 'c']);
object::remove({ a: 1 }, 'missing');
object::remove({ a: 1 }, []);
object::remove({ a: 1 }, ['a']);
object::remove({ a: 1 }, [1]);
object::remove({ a: 1 }, 123);
