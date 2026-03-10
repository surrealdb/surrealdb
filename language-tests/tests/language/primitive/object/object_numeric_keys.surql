/**
[test]

[test.results]
parsing-error = """
Failed to parse number: number cannot fit within a 64bit signed integer
  --> [19:5]
   |
19 | 18446744073709551615: "1",
   | ^^^^^^^^^^^^^^^^^^^^
"""


*/

{
    0: "0",
    1: "1",
    18446744073709551615: "1",
}
