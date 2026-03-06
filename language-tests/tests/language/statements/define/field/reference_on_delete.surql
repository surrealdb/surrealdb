/**
[test]
reason = "Test DEFINE FIELD with REF and ON DELETE strategies"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { author: 'DEFINE FIELD author ON post TYPE record<person> REFERENCE ON DELETE CASCADE PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { parent: 'DEFINE FIELD parent ON comment TYPE record<post> REFERENCE ON DELETE REJECT PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { item: 'DEFINE FIELD item ON tag TYPE record<post> REFERENCE ON DELETE IGNORE PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { target: 'DEFINE FIELD target ON link TYPE record<post> REFERENCE ON DELETE UNSET PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

*/

DEFINE TABLE person;
DEFINE TABLE post;
DEFINE FIELD author ON post TYPE record<person> REFERENCE ON DELETE CASCADE;
INFO FOR TABLE post;
DEFINE TABLE comment;
DEFINE FIELD parent ON comment TYPE record<post> REFERENCE ON DELETE REJECT;
INFO FOR TABLE comment;
DEFINE TABLE tag;
DEFINE FIELD item ON tag TYPE record<post> REFERENCE ON DELETE IGNORE;
INFO FOR TABLE tag;
DEFINE TABLE link;
DEFINE FIELD target ON link TYPE record<post> REFERENCE ON DELETE UNSET;
INFO FOR TABLE link;
