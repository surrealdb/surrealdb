/**
[test]

[[test.results]]
error = "Versioned error: A serialization error occured: serialising `Stream` directly is not supported"

*/

CREATE a:1 SET stream = b"".stream();
