/**
[test]

[[test.results]]
value = "[{ centre: (-0.118092f, 51.509865f), id: city:london }]"

[[test.results]]
value = "[{ centre: (-0.118092f, 51.509865f), id: city:london }]"

*/
UPSERT city:london SET centre = (-0.118092, 51.509865);
SELECT * FROM city:london;
