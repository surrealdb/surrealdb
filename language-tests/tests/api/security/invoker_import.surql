/**
[test]
run = false
reason = "Setup for invoker.surql and invoker_write.surql: a record access method, a table with row, field and write permissions, and APIs that read or write it under each security mode, directly and through api::invoke."
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM user WHERE id = $id )
	DURATION FOR SESSION 24h;
CREATE user:test;

DEFINE TABLE category SCHEMAFULL
	PERMISSIONS FOR select WHERE visible = true, FOR create, update, delete NONE;
DEFINE FIELD name ON category TYPE string;
DEFINE FIELD visible ON category TYPE bool;
DEFINE FIELD secret ON category TYPE string PERMISSIONS NONE;

CREATE category:shown SET name = "shown", visible = true, secret = "s1";
CREATE category:hidden SET name = "hidden", visible = false, secret = "s2";

// No SECURITY clause: DEFINER.
DEFINE API "/default/read"
	FOR get
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		};

DEFINE API "/definer/read"
	FOR get
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		}
	SECURITY DEFINER;

DEFINE API "/invoker/read"
	FOR get
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		}
	SECURITY INVOKER;

// The clause covers the whole API, so the fallback handler is subject to it.
DEFINE API "/invoker/fallback"
	FOR any
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		}
	SECURITY INVOKER;

// Either order of the trailing clauses is accepted.
DEFINE API "/invoker/comment_first"
	FOR get
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		}
	COMMENT "reads as the caller"
	SECURITY INVOKER;

// Middleware runs as the same party as the handler.
DEFINE API "/invoker/middleware"
	FOR get
		MIDDLEWARE api::res::header("x-probe", "1")
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		}
	SECURITY INVOKER;

DEFINE API "/altered/to_invoker"
	FOR get
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		};
ALTER API "/altered/to_invoker" SECURITY INVOKER;

DEFINE API "/altered/to_definer"
	FOR get
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		}
	SECURITY INVOKER;
ALTER API "/altered/to_definer" SECURITY DEFINER;

// Altering something else leaves the mode alone.
DEFINE API "/altered/comment_only"
	FOR get
		THEN {
			LET $categories = SELECT * FROM category ORDER BY id;
			RETURN { status: 200, body: $categories };
		}
	SECURITY INVOKER;
ALTER API "/altered/comment_only" COMMENT "unrelated";

// A handler runs as whoever called it, and an API called from a handler is
// called by that handler.
DEFINE API "/nested/definer_calls_invoker"
	FOR get
		THEN {
			RETURN { status: 200, body: api::invoke("/invoker/read").body };
		}
	SECURITY DEFINER;

DEFINE API "/nested/invoker_calls_definer"
	FOR get
		THEN {
			RETURN { status: 200, body: api::invoke("/definer/read").body };
		}
	SECURITY INVOKER;

DEFINE API "/nested/invoker_calls_invoker"
	FOR get
		THEN {
			RETURN { status: 200, body: api::invoke("/invoker/read").body };
		}
	SECURITY INVOKER;

// Writes. The table refuses every create, update and delete for the caller.
DEFINE API "/default/write"
	FOR post
		THEN {
			LET $created = CREATE category:injected SET name = "injected", visible = true, secret = "s3";
			RETURN { status: 200, body: $created };
		};

DEFINE API "/invoker/write"
	FOR post
		THEN {
			LET $created = CREATE category:injected SET name = "injected", visible = true, secret = "s3";
			RETURN { status: 200, body: $created };
		}
	SECURITY INVOKER;

DEFINE API "/invoker/update"
	FOR post
		THEN {
			LET $updated = UPDATE category:shown SET name = "renamed";
			RETURN { status: 200, body: $updated };
		}
	SECURITY INVOKER;

DEFINE API "/invoker/delete"
	FOR post
		THEN {
			LET $deleted = DELETE category:shown RETURN BEFORE;
			RETURN { status: 200, body: $deleted };
		}
	SECURITY INVOKER;
