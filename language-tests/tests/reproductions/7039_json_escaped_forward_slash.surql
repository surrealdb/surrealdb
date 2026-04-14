/**
[test]
reason = "Ensure JSON escaped forward slash (\\/) is parsed correctly per RFC 8259"
issue = 7039

[[test.results]]
value = "{ url: 'https://example.com/path' }"

[[test.results]]
value = "'https://example.com/path'"

[[test.results]]
value = "{ schedule_link: 'https://example.com/path', rating_link: 'https://example.com/review' }"

*/

-- Test escaped forward slashes in JSON object values
encoding::json::decode('{"url":"https:\\/\\/example.com\\/path"}');

-- Test escaped forward slashes in a plain JSON string
encoding::json::decode('"https:\\/\\/example.com\\/path"');

-- Test multiple fields with escaped forward slashes (matches the original issue)
encoding::json::decode('{"schedule_link":"https:\\/\\/example.com\\/path","rating_link":"https:\\/\\/example.com\\/review"}');
