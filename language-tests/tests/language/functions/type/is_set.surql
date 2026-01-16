/**
[test]
reason = "Test type::is::set function for type checking"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

*/

RETURN type::is_set({});
RETURN type::is_set(type::set([]));
RETURN type::is_set(type::set([1, 2, 3]));
RETURN type::is_set([1, 2, 3]);
RETURN type::is_set({ foo: 'bar' });
RETURN type::is_set('string');
RETURN type::is_set(123);
RETURN type::is_set(NULL);
RETURN {,}.is_set();