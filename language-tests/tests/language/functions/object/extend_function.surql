/**
[test]
reason = "Tests object::extend function-call form: shallow merge where the extension object wins on key conflicts (existing extend.surql only covers method form)"

[[test.results]]
value = "{ a: 1, b: 2 }"

[[test.results]]
value = "{ a: 1, b: 3 }"

[[test.results]]
value = "{ a: 1 }"

[[test.results]]
value = "{ a: 1 }"

[[test.results]]
value = "{ a: { y: 2 } }"

[[test.results]]
value = "{ a: 1, b: NONE }"

[[test.results]]
error = "Incorrect arguments for function object::extend(). Argument 2 was the wrong type. Expected `object` but found `'not an object'`"
*/

object::extend({ a: 1 }, { b: 2 });
object::extend({ a: 1, b: 2 }, { b: 3 });
object::extend({ a: 1 }, {});
object::extend({}, { a: 1 });
object::extend({ a: { x: 1 } }, { a: { y: 2 } });
object::extend({ a: 1 }, { b: NONE });
object::extend({ a: 1 }, 'not an object');
