/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: foo:74sp683iix2xr4ayj1bb, name: 'John Doe' }]"
rough = true

[[test.results]]
value = "[{ id: foo:iu30ga1jqayp38guxel4, name: 'Jane Doe' }]"
rough = true

*/
DEFINE INDEX national_id_idx ON foo FIELDS national_id UNIQUE;
CREATE foo SET name = 'John Doe';
CREATE foo SET name = 'Jane Doe';
