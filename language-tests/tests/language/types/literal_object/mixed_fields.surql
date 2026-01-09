/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: mixed:1, user: { email: 'test@example.com', id: 'user123' } }]"

[[test.results]]
value = "[{ id: mixed:2, user: { email: 'admin@example.com', id: 'admin456', name: 'Admin User' } }]"

[[test.results]]
error = "Couldn't coerce value for field `user` of `mixed:3`: Expected `{ email: string, id: string, name: none | string }` but found `{ id: 'user789' }`"

[[test.results]]
error = "Couldn't coerce value for field `user` of `mixed:4`: Expected `{ email: string, id: string, name: none | string }` but found `{ email: 'user@example.com', id: 'user999', name: 'Test', role: 'admin' }`"

*/
define field user on mixed type {
    id: string,
    email: string,
    name: option<string>
};

create mixed:1 set user = { id: "user123", email: "test@example.com" };
create mixed:2 set user = { id: "admin456", email: "admin@example.com", name: "Admin User" };
create mixed:3 set user = { id: "user789" }; // Missing required 'email'
create mixed:4 set user = { id: "user999", email: "user@example.com", name: "Test", role: "admin" }; // Extra 'role'

