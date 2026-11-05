/**
[test]
reason = "Tests meta::tb extracting the table part of record ids"

[[test.results]]
value = "'person'"

[[test.results]]
value = "'person'"

[[test.results]]
value = "'my table'"

[[test.results]]
value = "'person'"

[[test.results]]
error = "Incorrect arguments for function meta::tb(). Argument 1 was the wrong type. Expected `record` but found `'person:tobie'`"

[[test.results]]
error = "Incorrect arguments for function meta::tb(). Argument 1 was the wrong type. Expected `record` but found `123`"
*/

meta::tb(person:tobie);
meta::tb(person:100);
meta::tb(⟨my table⟩:1);
meta::tb(person:['London', 'New York']);
meta::tb("person:tobie");
meta::tb(123);
