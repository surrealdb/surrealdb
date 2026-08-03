/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for permission_predicate_document_root.surql: a table and a field
-- whose SELECT permission reaches the document root through `$parent` inside an
-- idiom-level filter, plus the record user the test signs in as. Runs as owner.
--
-- `alice` owns thing:1 and thing:3; `bob` owns thing:2. thing:3 carries an empty
-- `acl`, so its filter yields no elements and the predicate is falsy even though
-- alice owns it -- that separates "the predicate saw the row" from "the
-- predicate matched".
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN (
			SELECT * FROM user
			WHERE email = $email AND crypto::argon2::compare(pass, $pass)
		)
		DURATION FOR SESSION 1h;

	CREATE user:alice SET
		email = "alice@surrealdb.com",
		pass = crypto::argon2::generate("secret");
	CREATE user:bob SET
		email = "bob@surrealdb.com",
		pass = crypto::argon2::generate("secret");

	DEFINE TABLE thing SCHEMALESS PERMISSIONS
		FOR select WHERE acl[WHERE $parent.owner = $auth];

	DEFINE FIELD note ON thing PERMISSIONS
		FOR select WHERE tags[WHERE $parent.owner = $auth];

	CREATE thing:1 SET
		owner = user:alice, acl = [{ role: "read" }], tags = [{ t: "a" }], note = "n1";
	CREATE thing:2 SET
		owner = user:bob, acl = [{ role: "read" }], tags = [{ t: "b" }], note = "n2";
	CREATE thing:3 SET
		owner = user:alice, acl = [], tags = [{ t: "c" }], note = "n3";

	RETURN 'OK';
}
