/**
[test]

[test.results]
parsing-error = '''
Invalid operator '?~'
  --> [15:25]
   |
15 | ["abcde","bcde","cdef"] ?~ "cd";
   |                         ^^ The like operators have been removed. Please use the similarity functions, like string::similarity::smithwaterman, instead.
'''


*/
["abcde","bcde","cdef"] ?~ "cd";
["abcde","bcde","cdef"] ?~ "g";
["abcde","bcde","cdef"] ?~ "b";
