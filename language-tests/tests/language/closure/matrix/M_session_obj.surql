/**
[test]
reason = "The body is written in `RETURN [1].map(|$e| { ns: $session.ns != NONE })`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[{ ns: true }]"
*/
RETURN [1].map(|$e| { ns: $session.ns != NONE });
