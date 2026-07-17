/**
[test]
reason = "Tests string::similarity::sorensen_dice bigram coefficient including empty strings, sub-bigram inputs, whitespace handling, and type errors"

[[test.results]]
value = "1f"

[[test.results]]
value = "1f"

[[test.results]]
value = "0.25f"

[[test.results]]
value = "1f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0.8571428571428571f"

[[test.results]]
value = "0.4444444444444444f"

[[test.results]]
error = "Incorrect arguments for function string::similarity::sorensen_dice(). Argument 1 was the wrong type. Expected `string` but found `NONE`"
*/

string::similarity::sorensen_dice("", "");
string::similarity::sorensen_dice("night", "night");
string::similarity::sorensen_dice("night", "nacht");
string::similarity::sorensen_dice("a", "a");
string::similarity::sorensen_dice("a", "b");
string::similarity::sorensen_dice("word same", "same word");
string::similarity::sorensen_dice("héllo", "hello");
string::similarity::sorensen_dice(NONE, "abc");
