function() {
	return await surrealdb.query(`CREATE ONLY article:test SET name = "The daily news", issue_number = 3`)
};
SELECT * FROM article;
