SELECT * FROM foo
CREATE bar;
/* ===== result =====
ERROR:Error: Unexpected token `CREATE`, expected `;`
  |> ???:2:1
  |
1 | SELECT * FROM foo
  | ----------------- This last statement here was parsed as a statement expression
1 | SELECT * FROM foo
  |                  - Expected a `;` here
2 | CREATE bar;
  | ^^^^^^ Maybe missing a semicolon before this token?


*/