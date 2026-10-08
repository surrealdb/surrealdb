/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A recursion .{..} written as a function's argument, in a function body or in a COMPUTED field runs in the frame it is evaluated in"

[[test.results]]
value = "[{ id: folder:root }]"

[[test.results]]
value = "[{ id: folder:mid, parent: folder:root }]"

[[test.results]]
value = "[{ id: folder:leaf, parent: folder:mid }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[folder:mid, folder:root]"

[[test.results]]
value = "folder:root"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[folder:root]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[folder:mid, folder:root]"

[[test.results]]
value = "[[folder:mid, folder:root], [folder:root], []]"
*/
CREATE folder:root;
CREATE folder:mid SET parent = folder:root;
CREATE folder:leaf SET parent = folder:mid;
DEFINE FUNCTION fn::id($x: any) { RETURN $x };
RETURN fn::id(folder:leaf.{..+collect}(.parent));
RETURN fn::id(folder:leaf.{1..3}(.parent));
DEFINE FIELD anc ON folder COMPUTED parent.{..+collect}(.parent);
SELECT VALUE anc FROM folder:leaf;
DEFINE FUNCTION fn::anc($f: record) { RETURN $f.{..+collect}(.parent) };
RETURN fn::anc(folder:leaf);
SELECT VALUE fn::id(id.{..+collect}(.parent)) FROM folder ORDER BY id;
