/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

// Random number generater implemented in surrealql with a seed, allowing for deterministic random number generation.
// Bases on a well known GLSL random number generator, which was used because it does not rely on wrapping integers nor bit shifting
// both of which are not allowed in surrealql.

DEFINE FUNCTION overwrite fn::rand(){
	let $seed = SELECT value seed FROM ONLY __rand_state:1;
	let $seed = $seed ?? [0.0,0.5];
	let $rand = math::sin(vector::dot($seed,[12.9898,78.233])) * 43758.5453;
	let $rand = $rand - math::floor($rand);
	let $seed = if left {
		[$seed[0],$rand]
	}else{
		[$rand,$seed[1]]
	};

    return (UPSERT __rand_state:1 CONTENT {
            seed: $seed,
            left: !left,
    } RETURN VALUE seed[if left { 0 } else { 1 }])[0];
};

DEFINE FUNCTION overwrite fn::set_seed($seed: float){
    UPSERT __rand_state:1 CONTENT { seed: [$seed,0.5], left: false,  } RETURN NONE;
}

