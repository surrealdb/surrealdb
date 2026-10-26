/**
[env]
imports = ["./geo-points-100000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE INDEX place_location ON place FIELDS location SPATIAL;
	DEFINE INDEX place_category_location ON place FIELDS category, location SPATIAL;
	RETURN 'OK';
}
