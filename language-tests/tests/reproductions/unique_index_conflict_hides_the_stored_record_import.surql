/**
[test]
reason = """
	Setup for unique_index_conflict_hides_the_stored_record. A `users` table a
	record user may create on but not SELECT, with a UNIQUE index on `email`,
	seeded with a hidden row. A record access signs the user in.

	A `trigger` table carries a synchronous event whose THEN writes the hidden
	email, so a record user's CREATE on it raises the conflict from inside an
	event clause. A `tag` the user may delete is referenced by a `users` row
	whose ON DELETE THEN rewrites its email to the hidden value, so deleting the
	tag raises the conflict from inside a reference repair. Both reach the caller
	only after the nested error is stringified.

	A `chain_a` event writes `chain_b`, whose own event writes the hidden email,
	so a CREATE on `chain_a` raises the conflict two event frames deep. Each event
	frame suppresses the read check, so the redaction must read the caller's
	recorded status rather than the innermost frame.

	Two `DEFINE API` endpoints run definer code with permissions off on the
	record user's behalf: `/register` writes the email the request names, and
	`/chain` creates a `chain_a` row, raising the conflict two event frames
	beneath the handler.
"""
run = false
*/

DEFINE TABLE users SCHEMALESS PERMISSIONS
	FOR create, update FULL,
	FOR select NONE;
DEFINE FIELD email ON users TYPE string;
DEFINE INDEX email_idx ON users FIELDS email UNIQUE;

DEFINE TABLE tag SCHEMALESS PERMISSIONS FOR create, update, delete, select FULL;
DEFINE FIELD fav ON users TYPE option<record<tag>>
	REFERENCE ON DELETE THEN (UPDATE $this SET email = 'secret@example.com');

DEFINE TABLE trigger SCHEMALESS PERMISSIONS
	FOR create, update FULL,
	FOR select NONE;
DEFINE EVENT conflict ON trigger WHEN $event = "CREATE" THEN {
	CREATE users:event_victim SET email = 'secret@example.com';
};

DEFINE TABLE chain_a SCHEMALESS PERMISSIONS
	FOR create, update FULL,
	FOR select NONE;
DEFINE TABLE chain_b SCHEMALESS PERMISSIONS
	FOR create, update FULL,
	FOR select NONE;
DEFINE EVENT cascade_a ON chain_a WHEN $event = "CREATE" THEN {
	CREATE chain_b:nested;
};
DEFINE EVENT cascade_b ON chain_b WHEN $event = "CREATE" THEN {
	CREATE users:nested_victim SET email = 'secret@example.com';
};

DEFINE API "/register" FOR post THEN {
	CREATE users SET email = $request.body.email;
	{ status: 201 };
};
DEFINE API "/chain" FOR post THEN {
	CREATE chain_a:api_probe;
	{ status: 201 };
};

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:alice;
CREATE users:victim SET email = 'secret@example.com';

CREATE tag:fav;
CREATE users:referrer SET email = 'referrer@example.com', fav = tag:fav;
