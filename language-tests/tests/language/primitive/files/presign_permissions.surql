/**

[test]

[env]
imports = ["./util/presign_buckets.surql"]

[env.auth]
namespace = "test"
database = "test"
access = "acc"
rid = "person:tobie"

# 0: Minting a download URL within the cap is allowed.
[[test.results]]
value = "'GET'"

# 1: Past the cap it is not. `$expires` is bound for this action, so the clause
#    bounds the lifetime of what it hands out rather than only who may ask.
[[test.results]]
error = true
match = "$error = /permission to presign_get/"

# 2: A clause that names no presign_put arm mints no upload URLs, even though
#    it would allow a direct put. Presigning is its own action because a signed
#    URL outlives the session that made it.
[[test.results]]
error = true
match = "$error = /permission to presign_put/"

# 3: Within both caps, allowed.
[[test.results]]
value = "'PUT'"

# 4: Over the size cap, refused.
[[test.results]]
error = true
match = "$error = /permission to presign_put/"

# 5: **`max_size` omitted is refused too.** The option defaults to the node's
#    ceiling rather than to NONE, because `NONE <= 5242880` is true in
#    SurrealQL: a clause capping uploads would otherwise pass for an upload
#    with no cap at all, which is the one direction that must not fail open.
[[test.results]]
error = true
match = "$error = /permission to presign_put/"

# 6: `overwrite` defaults to true, so a clause requiring false refuses the
#    default.
[[test.results]]
error = true
match = "$error = /permission to presign_put/"

# 7: A listed type, allowed, and the URL tells the uploader which type to send.
[[test.results]]
value = "'image/png'"

# 8: An unlisted type, refused. `$content_type` is bound for presign_put, so a
#    clause decides which types a bearer URL may store.
[[test.results]]
error = true
match = "$error = /permission to presign_put/"

# 9: No type at all is refused by a clause that lists types: `$content_type` is
#    NONE, and `NONE IN [...]` is false.
[[test.results]]
error = true
match = "$error = /permission to presign_put/"

*/

RETURN file::presign_get(type::file("reads", "/a.png"), { expires: 10m }).method;

RETURN file::presign_get(type::file("reads", "/a.png"), { expires: 30m });

RETURN file::presign_put(type::file("reads", "/a.png"), { expires: 1m, max_size: 1 });

RETURN file::presign_put(type::file("writes", "/a.png"), { max_size: 1048576, overwrite: false }).method;

RETURN file::presign_put(type::file("writes", "/a.png"), { max_size: 99999999, overwrite: false });

RETURN file::presign_put(type::file("writes", "/a.png"), { overwrite: false });

RETURN file::presign_put(type::file("writes", "/a.png"), { max_size: 1048576 });

RETURN file::presign_put(type::file("images", "/a.png"), { content_type: "image/png" }).content_type;

RETURN file::presign_put(type::file("images", "/a.html"), { content_type: "text/html" });

RETURN file::presign_put(type::file("images", "/a.png"));
