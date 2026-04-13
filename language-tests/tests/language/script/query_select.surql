/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ number: 1f }]"

[[test.results]]
value = "[{ number: 2f }]"

*/
{
	CREATE test SET name = "a", number = 0;
	CREATE test SET name = "b", number = 1;
	CREATE test SET name = "c", number = 2;
	RETURN "OK";
};

function(){
	return await surrealdb.query(`SELECT number FROM test WHERE name = $name`,{
		name: "b"
	})
};

function(){
	let query = new surrealdb.Query(`SELECT number FROM test WHERE name = $name`);
	query.bind("name","c")
	return await surrealdb.query(query);
};
