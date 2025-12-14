/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: req:1, data: { age: 30, name: 'Alice' } }]"

[[test.results]]
error = "Couldn't coerce value for field `data` of `req:2`: Expected `{ age: int, name: string }` but found `{ name: 'Bob' }`"

[[test.results]]
error = "Couldn't coerce value for field `data` of `req:3`: Expected `{ age: int, name: string }` but found `{ age: 25, extra: 'not allowed', name: 'Charlie' }`"

[[test.results]]
error = "Couldn't coerce value for field `data` of `req:4`: Expected `{ age: int, name: string }` but found `{ age: 'thirty', name: 'David' }`"

*/
define field data on req type {
    name: string,
    age: int
};

create req:1 set data = { name: "Alice", age: 30 };
create req:2 set data = { name: "Bob" }; // Missing required field 'age'
create req:3 set data = { name: "Charlie", age: 25, extra: "not allowed" }; // Extra key
create req:4 set data = { name: "David", age: "thirty" }; // Wrong type

