/**
[test]
reason = "Tests type::array casting from arrays, sets, ranges, bytes, and invalid inputs"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[0, 1, 2, 3, 4]"

[[test.results]]
value = "[97, 98, 99]"

[[test.results]]
error = "Casting range `1..` to an array would create an array larger then the max allocation limit."

[[test.results]]
error = "Could not cast into `array` using input `'hello'`"

[[test.results]]
error = "Could not cast into `array` using input `NONE`"
*/

type::array([1, 2, 3]);
type::array([]);
type::array(<set>[1, 2, 2, 3]);
type::array(0..5);
type::array(<bytes>"abc");
type::array(1..);
type::array("hello");
type::array(NONE);
