/**
[env]
namespace = true
database = true

[test]
reason = "A destructure nests one `DestructurePart::Destructure` per level, and the idiom parser charges only the outer path part. The parse itself recurses on the heap, but lowering, `ToSql` and `Drop` each recurse per level on the native stack, so an unbounded nest aborts the process instead of reporting a parse error."

[test.results]
parsing-error = true
*/

-- ~130 nesting levels, past the default expression depth limit (128); a few
-- hundred bytes of text, far under any size limit.
RETURN a.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b.{b}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}};
