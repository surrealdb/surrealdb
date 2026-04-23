/**
[test]

[[test.results]]
value = "[{ field: { key: 'value' }, id: person:one }]"

[[test.results]]
value = "[{ field: { key: 'value' }, id: person:two }]"

*/
UPSERT person:one SET field.key = 'value';
UPSERT person:two SET field['key'] = 'value';
