/**
[env]
namespace = true
database = true

# Shorter than the 32-byte minimum, so the node holds no signing secret. The
# harness configures a valid one for every other test; this overrides it.
[env.config]
bucket_presign_secret = "too-short"

[test]
reason = "A node with no usable SURREAL_BUCKET_PRESIGN_SECRET refuses to mint rather than falling back to a per-node key, and a secret below the minimum length counts as none."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Presigned URLs are unavailable: this node has no URL-signing secret. Set SURREAL_BUCKET_PRESIGN_SECRET to the same value of at least 32 bytes on every node"

[[test.results]]
error = "Presigned URLs are unavailable: this node has no URL-signing secret. Set SURREAL_BUCKET_PRESIGN_SECRET to the same value of at least 32 bytes on every node"

*/

DEFINE BUCKET images BACKEND "memory";
file::presign_get(f"images:/a.png");
file::presign_put(f"images:/a.png");
