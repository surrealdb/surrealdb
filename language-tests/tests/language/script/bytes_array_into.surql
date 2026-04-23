/**
[test]

[[test.results]]
value = "NONE"

*/
RETURN function(<bytes> "hello world") {
	let arg = arguments[0];
	if (!(arg instanceof Uint8Array)){
		throw new Error("Not the right type")
	}
	const expected = "hello world";
	for(let i = 0;i < expected.length;i++){
		if (arg[i] != expected.charCodeAt(i)){
			throw new Error(`bytes[${i}] is not the right value`)
		}
	}
}
