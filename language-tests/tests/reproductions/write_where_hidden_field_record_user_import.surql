/**
[test]
reason = "Setup for write_where_hidden_field_record_user.surql: tables a record user may update and delete, each holding a field the user may write but not select, half of them with an index over that field, a table whose SELECT permission hides a record, and the record user the test signs in as. Runs as owner."
run = false
*/

DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
	DURATION FOR TOKEN 15m, FOR SESSION 6h;
CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');

DEFINE TABLE plain SCHEMALESS PERMISSIONS FOR select, update, delete FULL FOR create NONE;
DEFINE FIELD hidden ON plain PERMISSIONS FOR select NONE FOR create, update FULL;
INSERT INTO plain [
	{ id: plain:1, hidden: 'x', shown: 1 },
	{ id: plain:2, hidden: 'y', shown: 1 },
	{ id: plain:3, hidden: 'x', shown: 2 }
];

DEFINE TABLE indexed SCHEMALESS PERMISSIONS FOR select, update, delete FULL FOR create NONE;
DEFINE FIELD hidden ON indexed PERMISSIONS FOR select NONE FOR create, update FULL;
DEFINE INDEX indexed_hidden ON indexed FIELDS hidden;
INSERT INTO indexed [
	{ id: indexed:1, hidden: 'x', shown: 1 },
	{ id: indexed:2, hidden: 'y', shown: 1 },
	{ id: indexed:3, hidden: 'x', shown: 2 }
];

DEFINE TABLE gone_eq SCHEMALESS PERMISSIONS FOR select, update, delete FULL FOR create NONE;
DEFINE FIELD hidden ON gone_eq PERMISSIONS FOR select NONE FOR create, update FULL;
INSERT INTO gone_eq [
	{ id: gone_eq:1, hidden: 'x', shown: 1 },
	{ id: gone_eq:2, hidden: 'y', shown: 1 },
	{ id: gone_eq:3, hidden: 'x', shown: 2 }
];

DEFINE TABLE gone_ne SCHEMALESS PERMISSIONS FOR select, update, delete FULL FOR create NONE;
DEFINE FIELD hidden ON gone_ne PERMISSIONS FOR select NONE FOR create, update FULL;
INSERT INTO gone_ne [
	{ id: gone_ne:1, hidden: 'x', shown: 1 },
	{ id: gone_ne:2, hidden: 'y', shown: 1 },
	{ id: gone_ne:3, hidden: 'x', shown: 2 }
];

DEFINE TABLE gone_eq_indexed SCHEMALESS PERMISSIONS FOR select, update, delete FULL FOR create NONE;
DEFINE FIELD hidden ON gone_eq_indexed PERMISSIONS FOR select NONE FOR create, update FULL;
DEFINE INDEX gone_eq_indexed_hidden ON gone_eq_indexed FIELDS hidden;
INSERT INTO gone_eq_indexed [
	{ id: gone_eq_indexed:1, hidden: 'x', shown: 1 },
	{ id: gone_eq_indexed:2, hidden: 'y', shown: 1 },
	{ id: gone_eq_indexed:3, hidden: 'x', shown: 2 }
];

DEFINE TABLE gone_ne_indexed SCHEMALESS PERMISSIONS FOR select, update, delete FULL FOR create NONE;
DEFINE FIELD hidden ON gone_ne_indexed PERMISSIONS FOR select NONE FOR create, update FULL;
DEFINE INDEX gone_ne_indexed_hidden ON gone_ne_indexed FIELDS hidden;
INSERT INTO gone_ne_indexed [
	{ id: gone_ne_indexed:1, hidden: 'x', shown: 1 },
	{ id: gone_ne_indexed:2, hidden: 'y', shown: 1 },
	{ id: gone_ne_indexed:3, hidden: 'x', shown: 2 }
];

DEFINE TABLE veiled SCHEMALESS PERMISSIONS FOR select WHERE shown = 2 FOR update, delete FULL FOR create NONE;
INSERT INTO veiled [
	{ id: veiled:1, shown: 1, hidden: 'x' },
	{ id: veiled:2, shown: 2, hidden: 'x' }
];
