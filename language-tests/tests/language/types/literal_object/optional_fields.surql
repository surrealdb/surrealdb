/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1, types: {  } }]"

[[test.results]]
value = "[{ id: test:2, types: { can: { hold: 'me' } } }]"

[[test.results]]
error = "Couldn't coerce value for field `types` of `test:3`: Expected `{ can: none | { hold: string } }` but found `{ will: { be: 'restricted' } }`"

[[test.results]]
error = "Couldn't coerce value for field `types` of `test:4`: Expected `{ can: none | { hold: string } }` but found `{ can: { not: 'be bypassed' } }`"

[[test.results]]
value = "[{ id: test:1, types: {  } }, { id: test:2, types: { can: { hold: 'me' } } }]"

*/
define field types on test type {
    can: option<{
        hold: string
    }>
};

create test:1 set types = {};
create test:2 set types = {
    can: {
        hold: "me"
    }
};

create test:3 set types = {
    will: {
        be: "restricted"
    }
};

create test:4 set types = {
    can: {
        not: "be bypassed"
    }
};

select * from test;

