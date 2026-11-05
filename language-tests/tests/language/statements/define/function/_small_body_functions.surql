/**
[test]
reason = "Setup for small_body_light_call.surql: functions whose bodies are small leaves, defined before the query that calls them, so its calls link their bodies."
run = false
*/

DEFINE FUNCTION fn::over($x: number) { LET $d = $x * 2; RETURN $d > 150; };
DEFINE FUNCTION fn::band($x: int) -> string { LET $b = IF $x < 10 { 'low' } ELSE IF $x < 100 { 'mid' } ELSE { 'high' }; RETURN $b; };
DEFINE FUNCTION fn::wrong($x: int) -> int { LET $s = <string> $x; RETURN $s; };
DEFINE FUNCTION fn::refuse($x: int) { IF $x > 2 { THROW 'too big: ' + <string> $x }; RETURN $x; };
DEFINE FUNCTION fn::overflow($x: int) { LET $y = $x * 9223372036854775807; RETURN $y; };
DEFINE FUNCTION fn::doc() { LET $n = name; RETURN $n + '!'; };
CREATE t:1 SET name = 'a', n = 80;
CREATE t:2 SET name = 'b', n = 5;
