/**
[test]
reason = "This query used to trigger a bug in the parser"

[[test.results]]
value = "{ coordinates: [NONE, NONE], type: 'Point' }"

*/
{ coordinates: [$location.lat, $location.lng], type: 'Point' };

