/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
-- The releases this suite starts from define these fields as written. The
-- current checkout defines a field nested in one that is not defined, or one
-- named by more than field names, `*` and indexes, only as an import replays
-- it, which is how such a definition reaches it.
OPTION IMPORT;
DEFINE FIELD basic ON tab;
DEFINE FIELD all[*] ON tab;
DEFINE FIELD index[1] ON tab;
DEFINE FIELD member.member ON tab;
DEFINE FIELD flatten... ON tab;
