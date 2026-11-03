/**

[test]

# 0: Define bucket
[[test.results]]
value = "NONE"

# 1: The returned object names every part of the URL separately, so a caller
#    can tell whether it is absolute rather than guessing, and the
#    `content_type` an upload must send.
[[test.results]]
value = "['content_type', 'expires', 'href', 'method', 'origin', 'path']"

# 2: No SURREAL_PUBLIC_ORIGIN is configured here, so `origin` is NONE and
#    `href` collapses to `path`. A caller that blindly prefixed its own origin
#    would double it once an operator sets one, which is why both fields exist.
[[test.results]]
value = "true"

# 3: `path` is always relative, whatever the configuration holds, and carries
#    the namespace, database and bucket — a presigned URL arrives with no
#    session to carry them.
[[test.results]]
value = "true"

# 4: A colon in a key is percent-encoded, so a key can never introduce a path
#    segment of its own.
[[test.results]]
value = "true"

# 5: presign_get names the method it signed.
[[test.results]]
value = "'GET'"

# 6: An upload URL carries its byte ceiling, which the route enforces as the
#    body streams in.
[[test.results]]
value = "true"

# 7: A lifetime past the node's ceiling is refused by name rather than silently
#    clamped: a caller that asked for a day must not believe it got one.
[[test.results]]
error = true
match = "$error = /A presigned URL cannot last/"

# 8: So is a byte ceiling past the node's.
[[test.results]]
error = true
match = "$error = /A presigned upload cannot allow/"

# 9: A misspelled option is an error, not something to ignore. Every option
#    narrows what a bearer credential may do, so a typo that is quietly dropped
#    hands out a URL more permissive than the one asked for.
[[test.results]]
error = true
match = "$error = /Unknown file::presign_put option/"

# 10: The method-call form reaches the same function.
[[test.results]]
value = "'GET'"

# 11: An option that describes an upload is refused on a download for the same
#     reason a typo is: the accepted set is the operation's own, not the union
#     of both, so `max_size` cannot be accepted and then dropped.
[[test.results]]
error = true
match = "$error = /Unknown file::presign_get option/"

# 12: A zero or sub-second expiry is refused rather than truncated to 0s.
[[test.results]]
error = true
match = "$error = /A presigned URL must last at least 1s/"

*/

DEFINE BUCKET images BACKEND "memory" PERMISSIONS FULL;

RETURN object::keys(file::presign_get(type::file("images", "/a.png")));

RETURN file::presign_get(type::file("images", "/a.png")).origin IS NONE
	AND file::presign_get(type::file("images", "/a.png")).href.starts_with("/ns/test/db/test/bucket/");

RETURN file::presign_get(type::file("images", "/a.png")).path
	.starts_with("/ns/test/db/test/bucket/images/a.png?");

RETURN file::presign_get(type::file("images", "/u/user:tobie/a.png")).path
	.starts_with("/ns/test/db/test/bucket/images/u/user%3Atobie/a.png?");

RETURN file::presign_get(type::file("images", "/a.png")).method;

RETURN file::presign_put(type::file("images", "/a.png"), { max_size: 1024 }).path.contains("max=1024");

RETURN file::presign_get(type::file("images", "/a.png"), { expires: 1d });

RETURN file::presign_put(type::file("images", "/a.png"), { max_size: 999999999999 });

RETURN file::presign_put(type::file("images", "/a.png"), { overwrit: false });

RETURN f"images:/a.png".presign_get().method;

RETURN file::presign_get(type::file("images", "/a.png"), { max_size: 1024 });

RETURN file::presign_get(type::file("images", "/a.png"), { expires: 500ms });
