/**
[test]

[[test.results]]
error = false

[[test.results]]
error = 'Expected a array<record<employee>, 5> but the array had 6 items'
*/

DEFINE FIELD employees ON TABLE team TYPE array<record<employee>, 5>;
CREATE team SET employees = [
	employee:one, 
	employee:two, 
	employee:three, 
	employee:four, 
	employee:five, 
	employee:doesnt_belong
];
