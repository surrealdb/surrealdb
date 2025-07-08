/**
[test]

[test.results]
parsing-error = '''
Invalid operator '~'
  --> [15:9]
   |
15 | "abcde" ~ "abcde";
   |         ^ The like operators have been removed. Please use the similarity functions, like string::similarity::smithwaterman, instead.
'''


*/
"abcde" ~ "abcde";
"abcde" ~ "abc";
"abcde" ~ "abcdef";
"abcde" ~ "b";

