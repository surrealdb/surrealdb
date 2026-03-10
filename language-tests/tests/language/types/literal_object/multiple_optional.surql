/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: opt:1, config: {  } }]"

[[test.results]]
value = "[{ id: opt:2, config: { debug: true } }]"

[[test.results]]
value = "[{ id: opt:3, config: { timeout: 5000 } }]"

[[test.results]]
value = "[{ id: opt:4, config: { debug: false, timeout: 3000, verbose: true } }]"

[[test.results]]
error = "Couldn't coerce value for field `config` of `opt:5`: Expected `{ debug: none | bool, timeout: none | int, verbose: none | bool }` but found `{ extra: 'field' }`"

*/
define field config on opt type {
    debug: option<bool>,
    timeout: option<int>,
    verbose: option<bool>
};

create opt:1 set config = {};
create opt:2 set config = { debug: true };
create opt:3 set config = { timeout: 5000 };
create opt:4 set config = { debug: false, timeout: 3000, verbose: true };
create opt:5 set config = { extra: "field" }; // Extra key not in type

