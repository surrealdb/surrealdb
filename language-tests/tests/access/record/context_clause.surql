/**
[env]
namespace = true
database = true
imports = ["access/record/context_clause_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	user: "alice",
}"""

[test]
reason = """
An access method's CONTEXT clause is evaluated once, at authentication time,
with $auth bound to the settled identity, and its whole result — no
into_record() — is stored on the session and read back as $session.data.

It is evaluated under the authenticated record's own permissions, unlike
SIGNIN/SIGNUP/AUTHENTICATE, precisely because the result is client-readable:
it can only ever materialise what the session could have selected for itself.

$session is in PROTECTED_PARAM_NAMES, so LET $session — at the top level or
inside a block — is refused, as is a client-supplied session variable of that
name.

That is the whole of the claim. $session.data is exactly as trustworthy as
$auth: no better, no worse. Both names can still be rebound by a FOR loop or a
closure parameter, and a permission evaluated inside one reads the rebound
value. That is a pre-existing, engine-wide property of parameter binding, and
every record permission written before this clause existed already depends on
$auth having it.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "['acme']"

[[test.results]]
value = "[doc:1]"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
value = "['acme']"

*/

-- $auth is untouched by the clause: still a bare record link.
RETURN $auth;

-- The clause saw $auth, so it materialised only alice's grants.
RETURN $session.data;

-- A permission filtering on the payload returns exactly the granted rows.
SELECT VALUE id FROM doc ORDER BY id;

-- $session cannot be rebound by a LET, at the top level or inside a block.
LET $session = { data: ["globex"] };
{ LET $session = { data: ["globex"] }; RETURN $session.data };

-- ...and the real binding is still in place afterwards.
RETURN $session.data;
