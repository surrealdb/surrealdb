/**
[env]
namespace = true
database = true

[test]
reason = "A content type is stored only when given, never inferred from the key, reported by file::head but not by file::list, normalised so application/octet-stream means none, and refused rather than dropped when invalid or misspelled."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'image/png'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[NONE, NONE, NONE]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'image/png'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'text/plain; charset=utf-8'"

[[test.results]]
error = "Incorrect arguments for function file::put(). `not a type` is not a valid content type"

[[test.results]]
error = "Incorrect arguments for function file::put(). Unknown option `content_typo`, expected: content_type"

[[test.results]]
error = """
Incorrect arguments for function file::put_if_not_exists(). `text/plain\r
X-Evil: 1` is not a valid content type"""

[[test.results]]
value = "'image/png'"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

*/

DEFINE BUCKET b BACKEND "memory";

-- Stored when given, and reported by file::head.
file::put(f"b:/a.png", "png", { content_type: "image/png" });
file::head(f"b:/a.png").content_type;
-- Never inferred: a key ending in `.png` written without a type has none.
file::put(f"b:/b.png", "png");
file::head(f"b:/b.png").content_type;
-- `application/octet-stream` is the same as naming none.
file::put(f"b:/c.bin", "x", { content_type: "application/octet-stream" });
file::head(f"b:/c.bin").content_type;
-- A listing never reports a type: S3, GCS and Azure listings carry none.
file::list("b").map(|$e| $e.content_type);
-- A copy within the bucket keeps it.
file::copy(f"b:/a.png", "/d.png");
file::head(f"b:/d.png").content_type;
-- A write without one replaces the type with the object.
file::put(f"b:/a.png", "raw");
file::head(f"b:/a.png").content_type;
-- The method form takes the same option.
f"b:/e.txt".put("hi", { content_type: "text/plain; charset=utf-8" });
f"b:/e.txt".head().content_type;
-- Refused, not repaired.
file::put(f"b:/f", "x", { content_type: "not a type" });
file::put(f"b:/f", "x", { content_typo: "image/png" });
file::put_if_not_exists(f"b:/f", "x", { content_type: "text/plain\r\nX-Evil: 1" });
-- A presigned upload URL binds the type and reports it.
file::presign_put(f"b:/g.png", { content_type: "image/png" }).content_type;
string::contains(file::presign_put(f"b:/g.png", { content_type: "image/png" }).path, "ct=image%2Fpng&");
file::presign_put(f"b:/g.png").content_type;
