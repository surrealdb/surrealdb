/**
[env]
namespace = true
database = true

[test]
reason = "An `f\"bucket:/key\"` literal stops the key at a second colon and drops the rest without an error, so a key containing a colon silently addresses a different object. `type::file()` accepts the same key correctly, which shows the loss is in the literal's lexer and not in the file value."
# TODO: the assertions below state the contract `f"bucket:/key"` must uphold --
# the key is everything after the first colon -- and the lexer does not uphold
# it, so the test is filtered out of the run rather than asserted. It needs an
# issue; until it has one, `0_` is the reproductions prefix for a repro with no
# issue number.
wip = true

[[test.results]]
value = '"/u/user:tobie/avatar.png"'

[[test.results]]
value = '"/u/user:tobie/avatar.png"'

[[test.results]]
value = '"/a:b"'

*/

-- A colon is a legal character in an object key, and a natural one: a key
-- derived from a record id contains one. `type::file()` keeps it.
RETURN file::key(type::file("images", "/u/user:tobie/avatar.png"));

-- The literal form of the same file must produce the same key. It currently
-- yields "/u/user": the lexer ends the key at the second colon and discards
-- ":tobie/avatar.png" with no diagnostic.
--
-- Silence is what makes this dangerous rather than merely limiting. A caller
-- writing `file::put(f"images:/u/user:tobie/avatar.png", $data)` writes to
-- `/u/user`, and a later read through the same literal reads it back, so the
-- object appears to round-trip while every other client sees the wrong key.
-- Presigned URLs make it worse: the URL is minted for the truncated key, so it
-- grants access to an object the caller never named.
--
-- Either behaviour would be defensible; this one is not. The key should accept
-- a colon, or the literal should be a parse error the way `f"foo:bar"` already
-- is -- see language-tests/tests/parsing/file/error/invalid_separator.surql.
RETURN file::key(f"images:/u/user:tobie/avatar.png");

-- The minimal case, with nothing after the second colon to hide the loss.
RETURN file::key(f"images:/a:b");
