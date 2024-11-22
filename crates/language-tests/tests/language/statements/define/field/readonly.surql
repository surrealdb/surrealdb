DEFINE TABLE foo;
DEFINE FIELD bar ON foo READONLY;

create foo:1 content { bar: 1 };
