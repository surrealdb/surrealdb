/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A SELECT over the records a graph traversal reaches, ordered by an indexed field, sorts the reached records: the index lists the whole table and cannot order a traversal's subset, and WITH INDEX names no index of the source. The rows are the traversal's, in the order the field gives."
issue = 4880

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: language:eng }]"

[[test.results]]
value = "[{ id: language:fra }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ first_name: 'u2', stats: { post_count: 4 } }, { first_name: 'u5', stats: { post_count: 5 } }, { first_name: 'u8', stats: { post_count: 6 } }, { first_name: 'u1', stats: { post_count: 7 } }, { first_name: 'u4', stats: { post_count: 8 } }, { first_name: 'u7', stats: { post_count: 9 } }]"

[[test.results]]
value = "[{ first_name: 'u2', stats: { post_count: 4 } }, { first_name: 'u5', stats: { post_count: 5 } }, { first_name: 'u8', stats: { post_count: 6 } }, { first_name: 'u1', stats: { post_count: 7 } }, { first_name: 'u4', stats: { post_count: 8 } }, { first_name: 'u7', stats: { post_count: 9 } }]"

[[test.results]]
value = "{ children: [{ attributes: { sort_keys: 'stats.post_count ASC' }, children: [{ attributes: { expr: '(language:eng)<-speaks<-user' }, context: 'Rt', operator: 'Values' }], context: 'Db', operator: 'Sort' }], context: 'Db', operator: 'Project' }"
*/

DEFINE INDEX post_count ON TABLE user COLUMNS stats.post_count;
CREATE language:eng; CREATE language:fra;
FOR $i IN 0..9 {
	CREATE type::record('user', $i) SET first_name = 'u' + <string>$i, stats = { post_count: ($i * 7) % 10 };
	RELATE (type::record('user', $i))->speaks->(IF $i % 3 = 0 { language:fra } ELSE { language:eng });
};

SELECT first_name, stats.post_count FROM language:eng<-speaks<-user ORDER BY stats.post_count;
SELECT first_name, stats.post_count FROM language:eng<-speaks<-user WITH INDEX post_count ORDER BY stats.post_count;
SELECT first_name, stats.post_count FROM language:eng<-speaks<-user ORDER BY stats.post_count EXPLAIN;
