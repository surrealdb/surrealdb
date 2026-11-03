/**
# Buckets for `presign_permissions.surql`. Defined here rather than in that
# test because it runs as a record user, which cannot execute `DEFINE BUCKET`.
# `run = false` keeps this from being collected as a test of its own.

[test]
run = false
*/

-- Downloads only, and only for the lifetimes this clause allows. `$expires` is
-- bound for `presign_get`, so the clause bounds what it hands out and not only
-- who may ask for it.
DEFINE BUCKET reads BACKEND "memory"
	PERMISSIONS WHERE $action = "presign_get" AND $expires <= 15m;

-- Uploads only, capped in size and forbidden from replacing an existing
-- object.
DEFINE BUCKET writes BACKEND "memory"
	PERMISSIONS WHERE $action = "presign_put"
		AND $max_size <= 5242880
		AND $overwrite = false;

-- Uploads only of the media types listed. `$content_type` is NONE for a URL
-- that binds no type, and `NONE IN [...]` is false, so omitting the option is
-- refused rather than let through.
DEFINE BUCKET images BACKEND "memory"
	PERMISSIONS WHERE $action = "presign_put"
		AND $content_type IN ["image/png", "image/jpeg"];
