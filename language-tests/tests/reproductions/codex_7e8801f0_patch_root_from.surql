/**
[env]
namespace = true
database = true

[test]
reason = "JSON Patch copy/move with empty 'from' pointer must be rejected (Codex finding 7e8801f0). Without this guard, an attacker-supplied PATCH op `{ op: 'copy', from: '', path: '/leak' }` would copy the full record (including fields the caller is not permitted to SELECT) into an attacker-chosen destination, bypassing field-level SELECT permissions."

[[test.results]]
value = "[{ id: doc:1, public: 'visible', secret: 'hidden' }]"

[[test.results]]
error = "The JSON Patch contains invalid operations. Failed to parse JSON patch structure: 'copy' operation requires a non-root 'from' pointer"

[[test.results]]
error = "The JSON Patch contains invalid operations. Failed to parse JSON patch structure: 'move' operation requires a non-root 'from' pointer"
*/

CREATE doc:1 SET public = "visible", secret = "hidden";
UPDATE doc:1 PATCH [{ op: 'copy', from: '', path: '/leak' }];
UPDATE doc:1 PATCH [{ op: 'move', from: '', path: '/leak' }];
