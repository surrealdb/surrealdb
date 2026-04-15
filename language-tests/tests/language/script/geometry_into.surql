/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
let $param = (1f,2f);
RETURN function($param) {
	let arg = arguments[0];
	if(arg.type !== "Point"){
		throw new Error("Not the right type value")
	}
	if(!Array.isArray(arg.coordinates)){
		throw new Error("Not the right type coordinates: " + typeof arg.coordinates)
	}
	if(arg.coordinates[0] !== 1){
		throw new Error("Not the right coordinates value x = " + arg.coordinates[0])
	}
	if(arg.coordinates[1] !== 2){
		throw new Error("Not the right coordinates value y = " + arg.coordinates[1])
	}
}
