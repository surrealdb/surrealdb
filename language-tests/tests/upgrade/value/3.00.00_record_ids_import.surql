/**
[test]

[[test.results]]
value = "[{ id: rid:1 }]"

[[test.results]]
value = "[{ id: rid:abc }]"

[[test.results]]
value = "[{ id: rid:⟨hello world⟩ }]"

*/
CREATE rid:1;
CREATE rid:abc;
CREATE rid:`hello world`;
