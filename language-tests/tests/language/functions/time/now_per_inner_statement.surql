/**
[test]
reason = "A statement takes one instant, fixed when it starts: every clock read in it answers that instant, over many rows, in a data clause and in a DEFAULT alike. The statements of a block, of the body of a FOR or an IF, and of a function body each start at an instant of their own, so the clock advances from one statement of a list to the next, across iterations of a FOR, and across a SLEEP."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[true, true, true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[env]
namespace = "test"
database = "test"

*/

DEFINE FIELD at ON dflt TYPE datetime DEFAULT time::now();
DEFINE TABLE ix; DEFINE FIELD at ON ix TYPE datetime; DEFINE INDEX ix_at ON ix FIELDS at;
DEFINE FUNCTION fn::advances() { LET $a = time::now(); SLEEP 1ms; LET $b = time::now(); RETURN $a < $b; };
CREATE |row:20| RETURN NONE;
RETURN time::now() = time::now();
{ LET $a = [time::now(), time::now()]; $a[0] = $a[1] };
{ LET $a = time::now(); SLEEP 1ms; LET $b = time::now(); $a < $b };
{ LET $a = time::now(); LET $b = time::now(); $a <= $b };
RETURN count(array::distinct(SELECT VALUE time::now() FROM row)) = 1;
{ SLEEP 1ms; count(array::distinct(SELECT VALUE time::now() FROM row)) = 1 };
{ LET $a = SELECT VALUE time::now() FROM row; SLEEP 1ms; LET $b = SELECT VALUE time::now() FROM row; [count(array::distinct($a)) = 1, count(array::distinct($b)) = 1, $a[0] < $b[0]] };
FOR $i IN 1..=3 { SLEEP 1ms; CREATE tick SET n = $i, at = time::now(), again = time::now() };
{ LET $at = SELECT VALUE at FROM tick ORDER BY n; [$at[0] < $at[1], $at[1] < $at[2]] };
RETURN count(SELECT * FROM tick WHERE at != again);
FOR $i IN 1..=3 { SLEEP 1ms; LET $t = time::now(); SLEEP 1ms; CREATE stamp SET n = $i, at = $t, later = time::now() };
{ LET $at = SELECT VALUE at FROM stamp ORDER BY n; [$at[0] < $at[1], $at[1] < $at[2]] };
RETURN count(SELECT * FROM stamp WHERE at < later);
FOR $i IN 1..=3 { SLEEP 1ms; CREATE dflt SET n = $i };
{ LET $at = SELECT VALUE at FROM dflt ORDER BY n; [$at[0] < $at[1], $at[1] < $at[2]] };
RETURN fn::advances();
{ IF time::now() = time::now() { LET $a = time::now(); SLEEP 1ms; $a < time::now() } };
{ LET $a = time::now(); SLEEP 5ms; LET $r = (CREATE dflt SET n = 10)[0].at; $r > $a };
FOR $i IN 1..=3 { SLEEP 1ms; LET $r = (CREATE dflt SET n = 20 + $i)[0] };
{ LET $at = SELECT VALUE at FROM dflt WHERE n > 20 ORDER BY n; [$at[0] < $at[1], $at[1] < $at[2]] };
{ CREATE ix SET at = time::now(); SLEEP 5ms; count(SELECT * FROM ix WHERE at < time::now()) };
IF false { 1 } ELSE IF time::now() = time::now() { 2 } ELSE { 3 };
{ IF false { 1 } ELSE IF time::now() = time::now() { 2 } ELSE { 3 } };
