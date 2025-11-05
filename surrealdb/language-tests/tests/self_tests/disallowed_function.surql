/**
[env.capabilities]
deny-functions = ["array::add"]

[test]

[[test.results]]
error = "Function 'array::add' is not allowed to be executed"

*/
array::add([],1)
