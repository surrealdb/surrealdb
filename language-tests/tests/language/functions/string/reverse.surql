/**
[test]
reason = "Tests string::reverse reversing characters, including empty and unicode strings"

[[test.results]]
value = "'olleh'"

[[test.results]]
value = "''"

[[test.results]]
value = "'a'"

[[test.results]]
value = "'54321'"

[[test.results]]
value = "'dlröw olléh'"

[[test.results]]
value = "'double reversal'"

[[test.results]]
error = "Incorrect arguments for function string::reverse(). Argument 1 was the wrong type. Expected `string` but found `123`"
*/

string::reverse("hello");
string::reverse("");
string::reverse("a");
string::reverse("12345");
string::reverse("héllo wörld");
string::reverse(string::reverse("double reversal"));
string::reverse(123);
