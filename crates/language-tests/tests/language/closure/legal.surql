/**
[test]

[[test.results]]
value = "[{ closure: || 'legal', id: a:1 }]"

[env.capabilities]
insecure-storable-closures = true
*/

create a:1 set closure = || 'legal';