/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests nested fields with computed fields using wildcard syntax, $this context, and array<object> schema definitions"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `users.*.computed_name` as `COMPUTED` fields must be top-level."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: account:test, users: [{ first_name: 'Tobie', full_name: 'NONENONE', last_name: 'MH' }] }]"

[[test.results]]
value = "[{ id: account:test, users: [{ first_name: 'Tobie', full_name: 'NONENONE', last_name: 'MH' }] }]"

[[test.results]]
value = "[{ id: account:test, users: [{ first_name: 'Tobie', full_name: 'NONENONE', last_name: 'MH' }, { first_name: 'Jaime', full_name: 'NONENONE', last_name: 'MH' }] }]"

[[test.results]]
value = "[{ id: account:test, users: [{ first_name: 'Tobie', full_name: 'NONENONE', last_name: 'MH' }, { first_name: 'Jaime', full_name: 'NONENONE', last_name: 'MH' }] }]"

[[test.results]]
value = '''{ events: {  }, fields: { users: 'DEFINE FIELD users ON account TYPE array<object> PERMISSIONS FULL', "users.*": 'DEFINE FIELD users.* ON account TYPE object PERMISSIONS FULL', "users.*.first_name": 'DEFINE FIELD users.*.first_name ON account TYPE string PERMISSIONS FULL', "users.*.full_name": 'DEFINE FIELD users.*.full_name ON account VALUE string::concat($this.first_name, $this.last_name) PERMISSIONS FULL', "users.*.last_name": 'DEFINE FIELD users.*.last_name ON account TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }'''

*/

-- Define schema for array of user objects with computed fields
DEFINE FIELD users ON TABLE account TYPE array<object>;
DEFINE FIELD users.*.first_name ON TABLE account TYPE string;
DEFINE FIELD users.*.last_name ON TABLE account TYPE string;
DEFINE FIELD users.*.computed_name ON TABLE account COMPUTED
    string::concat($this.first_name, $this.last_name)
;
DEFINE FIELD users.*.full_name ON TABLE account VALUE
    string::concat($this.first_name, $this.last_name)
;

-- Test UPSERT with single user object
UPSERT account:test SET users = [
    { first_name: 'Tobie', last_name: 'MH' }
];

-- Verify the computed field was calculated
SELECT * FROM account:test;

-- Test with multiple user objects
UPSERT account:test SET users = [
    { first_name: 'Tobie', last_name: 'MH' },
    { first_name: 'Jaime', last_name: 'MH' }
];

-- Verify multiple computed fields
SELECT * FROM account:test;

-- Check schema information
INFO FOR TABLE account;
