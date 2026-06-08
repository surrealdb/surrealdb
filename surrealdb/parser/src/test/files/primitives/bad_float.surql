// There should atleast be whitespace between a float and other identifiers/kw.
SELECT 123fFROM bar;
/* ===== result =====
ERROR:Error: Invalid token
  |> ???:2:8
  |
2 | SELECT 123fFROM bar;
  |        ^^^^^ 


*/