RETURN function() {
	await Promise.all([
		surrealdb.query("1")
		surrealdb.query("1")
	])
}
