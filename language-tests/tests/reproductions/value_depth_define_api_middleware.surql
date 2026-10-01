/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A `DEFINE API ... MIDDLEWARE m(<expr>)` argument is a computed value stored in the middleware definition, which INFO and EXPORT render back recursively (converting each argument to a public value and formatting it). The definition is a plain revisioned encode with no depth guard of its own, so a middleware argument that nests past the cap would persist and then abort the process every time the API is read back. The argument here is built at the cap by an accepted fold and then wrapped once more by a plain array literal (not a fold), so it is the middleware-argument write guard - not the fold cap - that rejects it, before it can persist. The follow-up INFO shows no API was defined."

[[test.results]]
value = "NONE"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "{  }"

*/

LET $at_cap = array::fold(array::range(0, 255), [], |$a, $_| [$a]);
DEFINE API "/x"
	FOR get
		MIDDLEWARE api::req::max_body([$at_cap])
		PERMISSIONS FULL
		THEN {
			RETURN { status: 200, body: "OK" }
		};
(INFO FOR DB).apis;
