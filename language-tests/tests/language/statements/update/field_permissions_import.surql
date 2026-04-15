/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: data:1, private: 'private', public: 'public' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:1 }]"

*/
DEFINE TABLE data PERMISSIONS FULL;
DEFINE FIELD private ON data TYPE string PERMISSIONS FOR UPDATE FULL, FOR SELECT NONE;
CREATE data:1 SET public = "public", private = "private";

DEFINE ACCESS user ON DATABASE TYPE RECORD;
DEFINE TABLE user PERMISSIONS FULL;
CREATE user:1;
