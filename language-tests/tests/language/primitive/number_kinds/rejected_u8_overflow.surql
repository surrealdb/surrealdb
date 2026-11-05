/**
[test]

[test.results]
parsing-error = """
Failed to parse number: number cannot fit within an 8bit unsigned integer
  --> [14:1]
   |
14 | 256u8;
   | ^^^^^
"""
*/

256u8;
