/**
[test]
reason = "Tests meta::id extracting the key part of record ids of various key types"

[[test.results]]
value = "'tobie'"

[[test.results]]
value = "100"

[[test.results]]
value = "['London', 'New York']"

[[test.results]]
value = "{ city: 'London' }"

[[test.results]]
value = "'hello world'"

[[test.results]]
error = "Incorrect arguments for function meta::id(). Argument 1 was the wrong type. Expected `record` but found `'person:tobie'`"

[[test.results]]
error = "Incorrect arguments for function meta::id(). Argument 1 was the wrong type. Expected `record` but found `NONE`"
*/

meta::id(person:tobie);
meta::id(person:100);
meta::id(person:['London', 'New York']);
meta::id(person:{ city: 'London' });
meta::id(person:⟨hello world⟩);
meta::id("person:tobie");
meta::id(NONE);
