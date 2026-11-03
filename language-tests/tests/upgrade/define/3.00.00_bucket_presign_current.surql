/**
[env]
imports = ["upgrade/define/3.00.00_bucket_import.surql"]

[test]
upgrade = true
reason = """
A bucket a release wrote before `StoredBucketDefinition` revision 2 has no
`presign_nonce` and reads back as nil. That nil must be a usable identity: the
new binary mints a URL for the old bucket, and `ALTER BUCKET ... REVOKE
SIGNATURES` writes a real nonce into the old definition, which `INFO FOR DB
STRUCTURE` then renders."""
# Presigning and REVOKE SIGNATURES are the current checkout's.
version = ">=3.4.0-a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'GET'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

*/
(INFO FOR DB STRUCTURE).buckets.find(|$b| $b.name = "bkt").presign_nonce;
file::presign_get(f"bkt:/after.txt").method;
ALTER BUCKET bkt REVOKE SIGNATURES;
type::is_string((INFO FOR DB STRUCTURE).buckets.find(|$b| $b.name = "bkt").presign_nonce);
