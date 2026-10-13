/**
[test]
reason = "Tests type::uuid casting from uuids, parseable strings, and invalid inputs"

[[test.results]]
value = "u'ffffffff-ffff-ffff-ffff-ffffffffffff'"

[[test.results]]
value = "u'00000000-0000-0000-0000-000000000000'"

[[test.results]]
value = "u'01890a5d-ac96-774b-b970-036f7c2f7d15'"

[[test.results]]
error = "Could not cast into `uuid` using input `'not-a-uuid'`"

[[test.results]]
error = "Could not cast into `uuid` using input `123`"

[[test.results]]
error = "Could not cast into `uuid` using input `NONE`"
*/

type::uuid(u"ffffffff-ffff-ffff-ffff-ffffffffffff");
type::uuid(u"00000000-0000-0000-0000-000000000000");
type::uuid("01890a5d-ac96-774b-b970-036f7c2f7d15");
type::uuid("not-a-uuid");
type::uuid(123);
type::uuid(NONE);
