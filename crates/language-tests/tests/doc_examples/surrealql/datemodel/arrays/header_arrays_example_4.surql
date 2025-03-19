/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `employees` of `team:1ku9gw0kgmbc6z64o3r0`: Expected `array<record<employee>,5>` but found an collection of length `6`"
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
