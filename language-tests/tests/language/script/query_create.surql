/**
[test]

[[test.results]]
value = "{ id: article:test, issue_number: 3f, name: 'The daily news' }"

[[test.results]]
value = "[{ id: article:test, issue_number: 3, name: 'The daily news' }]"

*/
function() {
	return await surrealdb.query(`CREATE ONLY article:test SET name = "The daily news", issue_number = 3`)
};
SELECT * FROM article;
