/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: order:1, data: { a: 1, b: 2, c: 3, d: 4 } }]"

[[test.results]]
value = "[{ id: order:2, data: { a: 5, b: 6, c: 7, d: 8 } }]"

*/
define field data on order type {
    d: int,
    b: int,
    a: int,
    c: int
};

// Keys provided in different order than type definition
create order:1 set data = { a: 1, b: 2, c: 3, d: 4 };
create order:2 set data = { d: 8, c: 7, a: 5, b: 6 };

