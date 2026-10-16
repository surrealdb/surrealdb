/**
[env]
namespace = true
database = true

[test]
reason = "A filter over a SCHEMAFULL table whose own arithmetic on fields fails on some rows raises the first failing row's error, and no row after it is read, whichever fields the body reads of the rows before it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ big: 1, id: ip:1, n: 1, s: 'a' }]"

[[test.results]]
value = "[{ big: 2, id: ip:2, n: 2, s: 'b' }]"

[[test.results]]
value = "[{ big: 9223372036854775807, id: ip:3, n: 3, s: 'c' }]"

[[test.results]]
value = "[{ big: 9223372036854775807, id: ip:4, n: 4, s: 'd' }]"

[[test.results]]
value = "[{ big: 5, id: ip:5, n: 5, s: 'e' }]"

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '3'"

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '3'"

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '4'"

[[test.results]]
error = "Cannot perform multiplication with '9223372036854775807' and '3'"

[[test.results]]
value = "[ip:1, ip:2]"

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '3'"

[[test.results]]
value = "[ip:3, ip:4]"

[[test.results]]
error = "Cannot perform addition with '3' and '9223372036854775807'"
*/

DEFINE TABLE ip SCHEMAFULL;
DEFINE FIELD n ON ip TYPE int;
DEFINE FIELD big ON ip TYPE int;
DEFINE FIELD s ON ip TYPE string;
CREATE ip:1 CONTENT { n: 1, big: 1, s: 'a' };
CREATE ip:2 CONTENT { n: 2, big: 2, s: 'b' };
CREATE ip:3 CONTENT { n: 3, big: 9223372036854775807, s: 'c' };
CREATE ip:4 CONTENT { n: 4, big: 9223372036854775807, s: 'd' };
CREATE ip:5 CONTENT { n: 5, big: 5, s: 'e' };

SELECT VALUE n FROM ip WHERE big + n > 0;
SELECT * FROM ip WHERE n < 3 OR big + n > 0;
SELECT * FROM ip WHERE n > 3 AND big + n > 0;
SELECT VALUE id FROM ip WHERE big * n > 0;
SELECT VALUE id FROM ip WHERE n < 3 AND big + n > 0;
SELECT n, s FROM ip WHERE big + n > 0;
SELECT VALUE id FROM ip WHERE big - n > 0;
SELECT VALUE id FROM ip WHERE n + big < 0;
