// There should atleast be whitespace between a duration and other identifiers/kw.
SELECT 1h2dFROM bar;
/* ===== result =====
ERROR:Error: Invalid token
  |> ???:2:8
  |
2 | SELECT 1h2dFROM bar;
  |        ^^^^^ 


*/