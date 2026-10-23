/**
[env]
remote-skip = "asserts $token, which over the API is the claims of the harness's real root signin; the embedded harness runs with a connection-less session that has none"

[test]
reason = "The body is written in `RETURN [1].map(|$e| $token)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[NONE]"
*/
RETURN [1].map(|$e| $token);
