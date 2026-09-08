/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'alice@example.com', id: idx_uniq:1 }]"

[[test.results]]
value = "[{ email: 'bob@example.com', id: idx_uniq:2 }]"

*/
DEFINE INDEX idx_uniq_email ON TABLE idx_uniq COLUMNS email UNIQUE;
CREATE idx_uniq:1 SET email = 'alice@example.com';
CREATE idx_uniq:2 SET email = 'bob@example.com';
