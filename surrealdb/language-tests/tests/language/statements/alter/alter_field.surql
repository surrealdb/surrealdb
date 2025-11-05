/**

[env.capabilities]
allow-experimental = ["record_references"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON a PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ events: {  }, fields: { a: "DEFINE FIELD a ON a FLEXIBLE TYPE record<test> DEFAULT ALWAYS test:123 VALUE test:123 ASSERT true REFERENCE ON DELETE IGNORE COMMENT 'test' PERMISSIONS FOR select, create, update NONE" }, indexes: {  }, lives: {  }, tables: {  } }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON a PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

*/

DEFINE FIELD a ON a;
INFO FOR TB a;

ALTER FIELD a ON a
	FLEXIBLE
	TYPE record<test>
	READONLY
	VALUE test:123
	ASSERT true
	DEFAULT ALWAYS test:123
	PERMISSIONS NONE
	COMMENT "test"
	REFERENCE;
INFO FOR TB a;

ALTER FIELD a ON a
    DROP FLEXIBLE
    DROP TYPE
    DROP READONLY
    DROP VALUE
    DROP ASSERT
    DROP DEFAULT
    PERMISSIONS FULL
    DROP COMMENT
    DROP REFERENCE;
INFO FOR TB a;
