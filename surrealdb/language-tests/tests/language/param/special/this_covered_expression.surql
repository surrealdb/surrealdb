/**
[test]

[[test.results]]
value = "[{ direct: 'result', once: 'result', twice: 'result' }]"

*/

SELECT (($this.foo)) as twice, ($this.foo) as once, $this.foo as direct FROM [{ foo: "result" }];
