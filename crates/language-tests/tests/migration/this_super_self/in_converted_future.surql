/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found usage of special `$this` parameter, the semantics of which have changed', kind: 'this parent self parameter', location: { column: 67, kind: 'error', label: 'The value of this parameter might change in 3.0', length: 5, line: 1, source: 'COMPUTED {$this', truncation: 'start' }, origin: '/ns/test/db/testtb/t1/field/f/value', severity: 'might_break' }]"

*/

DEFINE FIELD f ON t1 VALUE <future> { $this };
migration::diagnose()
