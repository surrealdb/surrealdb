/**
[test]
reason = """
	An index addresses the key it renders as when the value beneath it is an
	object, so `a[0]` reaches `{ '0': .. }` — the rule a read already follows.
	A SPLIT on such a path splits, and the written key is the one the index names.
"""

[[test.results]]
value = '[{ a: { "0": 1 } }, { a: { "0": 2 } }]'

[[test.results]]
value = "[{ a: [1] }, { a: [2] }]"

[[test.results]]
value = "'hit'"

[[test.results]]
value = "['hit']"

[[test.results]]
value = "NONE"
*/

-- The split target reaches an object key.
SELECT * FROM [{ a: { '0': [1, 2] } }] SPLIT a[0];

-- The same target over an array, which addresses the element.
SELECT * FROM [{ a: [[1, 2]] }] SPLIT a[0];

-- A read of the same path, for the rule the split follows.
RETURN { '0': 'hit' }[0];
SELECT VALUE a[0] FROM [{ a: { '0': 'hit' } }];

-- `[0]` written as first, and `[$]`, address no key: an object has no order.
RETURN { '0': 'hit' }[$];
