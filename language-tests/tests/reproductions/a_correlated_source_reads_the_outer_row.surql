/**
[env]
namespace = true
database = true

[test]
reason = """
The `FROM` expression of a correlated subquery is evaluated against the outer
row, and that row is the *root* of the evaluation rather than only the value
the expression starts from. A nested expression — a method's argument, a
dynamic index — is evaluated against the root, so an outer field named from
inside one has to find the same row the receiver was read from; reading it
against the receiver instead leaves it `NONE`.

The two-row case is what pins it: each inner run splits on its own row's
separator, so the answers differ in length rather than both taking the first
row's value or neither taking any.
"""

[[test.results]]
value = "[[1, 1], [1, 1, 1]]"

[[test.results]]
value = "[[1, 1]]"

[[test.results]]
value = "[[1, 1]]"

[[test.results]]
value = "[['a', 'b']]"
*/

-- The separator comes from the same outer row as the string being split.
SELECT VALUE (SELECT VALUE 1 FROM $this.text.split($this.sep))
FROM [{ text: 'a-b', sep: '-' }, { text: 'c/d/e', sep: '/' }];

-- Control: a literal argument never needed the outer row.
SELECT VALUE (SELECT VALUE 1 FROM $this.text.split('-'))
FROM [{ text: 'a-b', sep: '-' }];

-- A dynamic index is the other nested read.
SELECT VALUE (SELECT VALUE 1 FROM $this.text[$this.sep])
FROM [{ text: { '-': [7, 8] }, sep: '-' }];

-- Inside the subquery `$this` is the inner row, so the outer row is reachable
-- only where the source expression itself is evaluated.
SELECT VALUE (SELECT VALUE $this FROM $this.text.split($this.sep))
FROM [{ text: 'a-b', sep: '-' }];
