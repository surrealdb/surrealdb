SELECT * FROM foo ODRER BY bla;
/* ===== result =====
ERROR:Error: Unexpected token `ODRER`, expected `;`
  |> ???:1:19
  |
1 | SELECT * FROM foo ODRER BY bla;
  |                   ^^^^^ Maybe missing a semicolon before this token?
1 | SELECT * FROM foo ODRER BY bla;
  | ----------------- This last statement here was parsed as a statement expression
1 | SELECT * FROM foo ODRER BY bla;
  |                  - Expected a `;` here


*/