/**
[test]
reason = "Chain mutation for field_reference.surql: every walked version adds one more book pointing at the author the first version wrote, so the Ref key family accumulates back-link entries written by successive binaries."
run = false
*/
CREATE type::record('chain_field_reference_book', $hop) SET
	title = <string> $hop,
	hop = $hop,
	author = chain_field_reference_author:seed,
	wrote_at = $version;
