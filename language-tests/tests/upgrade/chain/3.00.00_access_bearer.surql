/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Carries one BEARER access grant set through every version in the chain. The
first walked version defines the access and the user it grants for and issues
the first grant; every version after it appends a grant to the same grant
keyspace before reading the whole set back.

A pairwise hop can only show that one release enumerates the grants the release
directly before it wrote. This walk shows that a grant written at the head of
the chain is still enumerated, still bound to its user, still redacted, and
still live — and that the revocation the oldest binary stamped is still read as
one — after every release in turn has written into the same keyspace.

Grant identifiers and timestamps are generated, so the body asserts count and
shape and never an exact value."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_access_bearer_setup.surql"
mutate = "upgrade/chain/3.00.00_access_bearer_mutate.surql"

# The body runs on every walked release, and how much of an access a release
# renders is that release's own business — the `WITH JWT` clause is absent from
# the definition text of the older binaries in the matrix — so this brackets the
# durations rather than pinning the line.
[[test.results]]
match = '''
	string::contains($result.accesses.chain_access_bearer_ac, "DEFINE ACCESS chain_access_bearer_ac ON DATABASE TYPE BEARER FOR USER")
		&& string::contains($result.accesses.chain_access_bearer_ac, "DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE")
		&& string::contains($result.users.chain_access_bearer_user, "DEFINE USER chain_access_bearer_user ON DATABASE")
		&& string::contains($result.users.chain_access_bearer_user, "ROLES VIEWER")
'''

[[test.results]]
match = '''
	count($result) == $hop + 1
		&& count(array::distinct($result.id)) == $hop + 1
		&& count(array::distinct($result.grant.id)) == $hop + 1
		&& array::distinct($result.ac) == ['chain_access_bearer_ac']
		&& array::distinct($result.type) == ['bearer']
		&& array::distinct($result.subject) == [{ user: 'chain_access_bearer_user' }]
		&& array::distinct($result.grant.key) == ['[REDACTED]']
		&& array::all($result, |$g| type::is_string($g.id)
			AND type::is_datetime($g.creation)
			AND type::is_datetime($g.expiration))
'''

[[test.results]]
match = '''
	count($result) == $hop
		&& array::distinct($result.revocation) == [NONE]
'''

[[test.results]]
match = '''
	count($result) == 1
		&& type::is_datetime($result[0].revocation)
		&& $result[0].subject == { user: 'chain_access_bearer_user' }
		&& $result[0].grant.key == '[REDACTED]'
'''

*/
INFO FOR DB;
-- One grant per version walked so far, plus the one the head of the chain
-- issued and revoked: every release in turn appended to the set an older release
-- started.
ACCESS chain_access_bearer_ac ON DATABASE SHOW ALL;
-- Every grant issued by the walk is still live; the head of the chain's is not.
ACCESS chain_access_bearer_ac ON DATABASE SHOW WHERE revocation == NONE;
-- The revocation the first walked version stamped is the only one, and every
-- later binary still decodes it as a revocation rather than losing it.
ACCESS chain_access_bearer_ac ON DATABASE SHOW WHERE revocation != NONE;
