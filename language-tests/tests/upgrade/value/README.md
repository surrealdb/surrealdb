Upgrade tests for value encodings: the bytes a stored value occupies, rather
than the catalog entry describing it.

A test here is the same pair as anywhere else — `X_import.surql` writes records
on the old binary, `X.surql` reads them back on the new one — but what it pins is
the round trip of the value itself. See `../README.md` for the axes, the
frontmatter and the naming rule.

The distinction from `define/` is what breaks when the test fails. A definition
that stops decoding takes the whole table with it and is loud. A value encoding
that shifts is quiet: the record still loads, and one field comes back subtly
wrong — a set that lost its ordering, a geometry whose coordinates transposed, a
`NONE` that became `NULL`. Coverage here is therefore per value *type*, and each
test writes the awkward cases rather than a representative one: empty and
single-element containers, nested containers, the boundary values of numeric
types, and the difference between absent, `NONE` and `NULL`.

Two rules matter more here than elsewhere.

Assert the value, not its rendering. `INFO`-style string comparisons belong in
`define/`; a test here should read the record back and compare the value, so a
change in how a type is *displayed* does not masquerade as an encoding failure,
and vice versa.

Order is only assertable where the type guarantees it. Arrays keep their order
and may be compared directly. Sets and the results of a scan do not — sort before
comparing, or the test starts failing on a seed whose key layout happens to
differ.
