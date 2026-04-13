let $param = {
	type: "Point",
	coordinates: [1,2]
};

RETURN function($param) {
	let arg = arguments[0];
	if(arg.type !== "Point"){
		throw new Error("Not the right type value")
	}
	if(Array.isArray(arg.coordinates)){
		throw new Error("Not the right type coordinates")
	}
	if(arg.coordinates[0] === 1){
		throw new Error("Not the right coordinates value")
	}
	if(arg.coordinates[1] === 2){
		throw new Error("Not the right coordinates value")
	}
}
