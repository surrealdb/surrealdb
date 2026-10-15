/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "An analyzer FUNCTION clause names an extension function as `ext::` or its deprecated alias `mod::`. Both store the same reference, which renders as `ext::`."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE ANALYZER a FUNCTION ext::demo::lower TOKENIZERS CLASS', b: 'DEFINE ANALYZER b FUNCTION ext::demo::lower TOKENIZERS CLASS' }"
*/

-- 0
DEFINE ANALYZER a FUNCTION ext::demo::lower TOKENIZERS class;

-- 1
DEFINE ANALYZER b FUNCTION mod::demo::lower TOKENIZERS class;

-- 2
(INFO FOR DB).analyzers;
