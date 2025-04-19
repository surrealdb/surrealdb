/**
[test]

[[test.results]]
value = "[[{ direct: 'result', once: 'result', twice: 'result' }]]"

*/

SELECT VALUE (SELECT (($parent.value)) as twice, ($parent.value) as once, $parent.value as direct FROM [1]) FROM [{ value: "result" }]
