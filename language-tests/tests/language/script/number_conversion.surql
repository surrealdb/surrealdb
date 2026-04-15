/**
[test]

[[test.results]]
value = "{ a: 9007199254740991f, b: -9007199254740992f, c: 100000000000000000, d: 9223372036854775808dec }"

*/
RETURN function() {
	if(await surrealdb.value(`2147483647`) !== 2147483647){
		throw new Error(1)
	}
	if(await surrealdb.value(`9007199254740991`) !== 9007199254740991){
		throw new Error(2)
	}
	if(await surrealdb.value(`9007199254740992`) !== 9007199254740992n){
		throw new Error(3)
	}
	if(await surrealdb.value(`-9007199254740992`) !== -9007199254740992){
		throw new Error(4)
	}
	if(await surrealdb.value(`-9007199254740993`) !== -9007199254740993n){
		throw new Error(5)
	}
	return {
		a:  9007199254740991,
		b: -9007199254740992,
		c: 100000000000000000n,
		d: 9223372036854775808n
	}
}
