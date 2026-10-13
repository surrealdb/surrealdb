/**
[test]
reason = "Tests type::string_lossy lossy UTF-8 conversion of bytes and string casting of other values"

[[test.results]]
value = "'hello'"

[[test.results]]
value = "'héllo wörld'"

[[test.results]]
value = "'hi�'"

[[test.results]]
value = "''"

[[test.results]]
value = "'123'"

[[test.results]]
value = "'1.5f'"

[[test.results]]
value = "'true'"

[[test.results]]
value = "'NONE'"

[[test.results]]
value = "'NULL'"
*/

type::string_lossy("hello");
type::string_lossy(<bytes>"héllo wörld");
type::string_lossy(<bytes>[104, 105, 255]);
type::string_lossy(<bytes>[]);
type::string_lossy(123);
type::string_lossy(1.5);
type::string_lossy(true);
type::string_lossy(NONE);
type::string_lossy(NULL);
