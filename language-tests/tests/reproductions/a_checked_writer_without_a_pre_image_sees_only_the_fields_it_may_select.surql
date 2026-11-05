/**
[env]
imports = ["reproductions/a_checked_writer_without_a_pre_image_sees_only_the_fields_it_may_select_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "actors:writer" }

[test]
reason = """
A record the statement creates has no pre-image, and the snapshot a permission-checked caller's
data clause reads of it is cut by the guards on the table's stored fields. A `PATCH` pointer that
reads a field the caller may not select is refused even when an earlier operation of the same
patch wrote it, and a `SET` or `CONTENT` that names such a field reads it as `NONE`.
"""

[[test.results]]
error = "Cannot read `secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

[[test.results]]
error = "Cannot read `secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

[[test.results]]
error = "Cannot read `secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

[[test.results]]
value = "[{ copy: 'c' }]"

[[test.results]]
value = "[{ copy: NONE }]"

[[test.results]]
value = "[{ copy: NONE }]"

[[test.results]]
value = "[{ id: vault:7 }]"

[[test.results]]
value = "[{ secret: NONE }]"

[[test.results]]
value = "[{ copy: 'c', id: vault:4 }, { id: vault:5 }, { id: vault:6 }, { id: vault:7 }, { id: vault:8 }]"
*/

CREATE vault:1 PATCH [{ op: 'add', path: '/secret', value: 's' }, { op: 'copy', from: '/secret', path: '/copy' }] RETURN copy;
CREATE vault:2 PATCH [{ op: 'add', path: '/secret', value: 's' }, { op: 'move', from: '/secret', path: '/copy' }] RETURN copy;
CREATE vault:3 PATCH [{ op: 'add', path: '/secret', value: 's' }, { op: 'test', path: '/secret', value: 's' }] RETURN copy;
CREATE vault:4 PATCH [{ op: 'add', path: '/copy', value: 'c' }, { op: 'copy', from: '/copy', path: '/secret' }] RETURN copy;
CREATE vault:5 SET secret = 's', copy = secret RETURN copy;
CREATE vault:6 CONTENT { secret: 's', copy: secret } RETURN copy;
CREATE vault:7 SET secret = 's' RETURN AFTER;
CREATE vault:8 SET secret = 's' RETURN secret;
SELECT * FROM vault;
