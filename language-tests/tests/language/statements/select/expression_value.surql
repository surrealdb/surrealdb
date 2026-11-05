/**
[env]

[test]
reason = "Test SELECT VALUE with negation and boolean NOT expressions"

[[test.results]]
value = "[{ boolean: true, id: thing:a, number: 5 }]"

[[test.results]]
value = "[{ boolean: false, id: thing:b, number: -5 }]"

[[test.results]]
value = "[-5, 5]"

[[test.results]]
value = "[false, true]"

*/
CREATE thing:a SET number = 5, boolean = true;
CREATE thing:b SET number = -5, boolean = false;
SELECT VALUE -number FROM thing;
SELECT VALUE !boolean FROM thing;
