/**
[test]
reason = "Test record ID link traversal with various index types and join plans (new executor)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, name: 'h' }]"

[[test.results]]
value = "[{ id: t:2, name: 'h' }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
value = "[{ id: i:B, t: t:2 }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }, { id: i:B, t: t:2 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
  TableScan [ctx: Db] [table: i, direction: Forward, predicate: t.name = 'h'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, name: 'h' }]"

[[test.results]]
value = "[{ id: t:2, name: 'h' }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
value = "[{ id: i:B, t: t:2 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
  TableScan [ctx: Db] [table: i, direction: Forward, predicate: t.name = 'h'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: i:A, t: t:1 }, { id: i:B, t: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, name: 'h' }]"

[[test.results]]
value = "[{ id: t:2, name: 'h' }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
value = "[{ id: i:B, t: t:2 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
  TableScan [ctx: Db] [table: i, direction: Forward, predicate: t.name = 'h'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: i:A, t: t:1 }, { id: i:B, t: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, name: 'a' }]"

[[test.results]]
value = "[{ id: t:2, name: 'b' }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
value = "[{ id: i:B, t: t:2 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
  TableScan [ctx: Db] [table: i, direction: Forward, predicate: t.name INSIDE ['a', 'b']] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: i:A, t: t:1 }, { id: i:B, t: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, name: 'Hello World' }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
error = "Invalid query: New executor does not support: MATCHES with multi-part field path not yet supported in streaming executor"

[[test.results]]
error = "Invalid query: New executor does not support: MATCHES with multi-part field path not yet supported in streaming executor"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, name: 'Hello World' }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
error = "Invalid query: New executor does not support: MATCHES with multi-part field path not yet supported in streaming executor"

[[test.results]]
error = "Invalid query: New executor does not support: MATCHES with multi-part field path not yet supported in streaming executor"

[[test.results]]
error = "Invalid query: New executor does not support: MATCHES with multi-part field path not yet supported in streaming executor"

[[test.results]]
error = "Invalid query: New executor does not support: MATCHES with multi-part field path not yet supported in streaming executor"
*/

-- No index on link field
DEFINE FIELD name ON TABLE t TYPE string;
DEFINE FIELD t ON TABLE i TYPE record<t>;
CREATE t:1 SET name = 'h';
CREATE t:2 SET name = 'h';
CREATE i:A SET t = t:1;
CREATE i:B SET t = t:2;
SELECT * FROM i WHERE t.name = 'h';
EXPLAIN ANALYZE SELECT * FROM i WHERE t.name = 'h';

-- With standard index on link field
REMOVE TABLE i;
REMOVE TABLE t;
DEFINE INDEX i_t_id ON TABLE i COLUMNS t;
DEFINE INDEX t_name_idx ON TABLE t COLUMNS name;
DEFINE FIELD name ON TABLE t TYPE string;
DEFINE FIELD t ON TABLE i TYPE record<t>;
CREATE t:1 SET name = 'h';
CREATE t:2 SET name = 'h';
CREATE i:A SET t = t:1;
CREATE i:B SET t = t:2;
EXPLAIN ANALYZE SELECT * FROM i WHERE t.name = 'h';
SELECT * FROM i WHERE t.name = 'h';

-- With unique index on link field
REMOVE TABLE i;
REMOVE TABLE t;
DEFINE INDEX i_t_unique_id ON TABLE i COLUMNS t UNIQUE;
DEFINE INDEX t_name_idx ON TABLE t COLUMNS name;
DEFINE FIELD name ON TABLE t TYPE string;
DEFINE FIELD t ON TABLE i TYPE record<t>;
CREATE t:1 SET name = 'h';
CREATE t:2 SET name = 'h';
CREATE i:A SET t = t:1;
CREATE i:B SET t = t:2;
EXPLAIN ANALYZE SELECT * FROM i WHERE t.name = 'h';
SELECT * FROM i WHERE t.name = 'h';

-- With unique remote index and IN operator
REMOVE TABLE i;
REMOVE TABLE t;
DEFINE INDEX i_t_id ON TABLE i COLUMNS t;
DEFINE INDEX t_name_unique_idx ON TABLE t COLUMNS name UNIQUE;
DEFINE FIELD name ON TABLE t TYPE string;
DEFINE FIELD t ON TABLE i TYPE record<t>;
CREATE t:1 SET name = 'a';
CREATE t:2 SET name = 'b';
CREATE i:A SET t = t:1;
CREATE i:B SET t = t:2;
EXPLAIN ANALYZE SELECT * FROM i WHERE t.name IN ['a', 'b'];
SELECT * FROM i WHERE t.name IN ['a', 'b'];

-- With full-text index on remote field
REMOVE TABLE i;
REMOVE TABLE t;

DEFINE ANALYZER name TOKENIZERS class FILTERS lowercase,ngram(1,128);
DEFINE INDEX t_name_search_idx ON TABLE t COLUMNS name FULLTEXT ANALYZER name BM25 HIGHLIGHTS;
DEFINE INDEX i_t_id ON TABLE i COLUMNS t;
DEFINE FIELD name ON TABLE t TYPE string;
DEFINE FIELD t ON TABLE i TYPE record<t>;
CREATE t:1 SET name = 'Hello World';
CREATE i:A SET t = t:1;
EXPLAIN ANALYZE SELECT * FROM i WHERE t.name @@ 'world';
SELECT * FROM i WHERE t.name @@ 'world';

-- Full-text on remote field without local record index
REMOVE TABLE i;
REMOVE TABLE t;
DEFINE ANALYZER name2 TOKENIZERS class FILTERS lowercase,ngram(1,128);
DEFINE INDEX t_name_search_idx ON TABLE t COLUMNS name FULLTEXT ANALYZER name2 BM25 HIGHLIGHTS;
DEFINE FIELD name ON TABLE t TYPE string;
DEFINE FIELD t ON TABLE i TYPE record<t>;
CREATE t:1 SET name = 'Hello World';
CREATE i:A SET t = t:1;
EXPLAIN ANALYZE SELECT * FROM i WHERE t.name @@ 'world';
SELECT * FROM i WHERE t.name @@ 'world';
SELECT * FROM i WHERE t.name @OR@ 'world dummy';
SELECT * FROM i WHERE t.name @AND@ 'world hello';
