// There should atleast be whitespace between a decimal and other identifiers/kw.
SELECT 123decFROM bar;
/* ===== result =====
ERROR:Error: Invalid token
  |> ???:2:8
  |
2 | SELECT 123decFROM bar;
  |        ^^^^^^^ 


*/