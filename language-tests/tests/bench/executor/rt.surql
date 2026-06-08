/**
[env]
imports = ["./rt_import.surql"]

[bench]
reason = "Benching SurrealQL compute performance with a tiny raytracer"
run = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NULL"
*/

let $size = if $__bench__ {
	4
}else{
	2
};

let $image = CREATE ONLY image SET width = $size, height = $size RETURN VALUE id;
fn::trace($image);
