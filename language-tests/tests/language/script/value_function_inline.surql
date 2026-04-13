/**
[test]

[[test.results]]
value = "NONE"

*/
function() {
	if(await surrealdb.value(`3`) !== 3){
		throw new Error(1)
	}
	if(await surrealdb.value(`"some string"`) !== "some string"){
		throw new Error(2)
	}
	if(await surrealdb.value(`math::floor(13.746189)`) !== 13){
		throw new Error(3)
	}
}
