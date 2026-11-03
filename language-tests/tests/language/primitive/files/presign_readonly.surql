/**

[test]

# 0: Define a readonly bucket
[[test.results]]
value = "NONE"

# 1: A readonly bucket still mints download URLs.
[[test.results]]
value = "'GET'"

# 2: It must not mint an upload URL. The check is on minting, not on
#    redemption: a URL that cannot be used is still a URL that should never
#    have been handed out.
[[test.results]]
error = true
match = "$error = /read-only mode/"

*/

DEFINE BUCKET archive BACKEND "memory" READONLY PERMISSIONS FULL;

RETURN file::presign_get(type::file("archive", "/a.png")).method;

RETURN file::presign_put(type::file("archive", "/a.png"));
