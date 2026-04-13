{
	a: 1,
}
CREATE FOO;
/* ===== result =====
ERROR:Error: Unexpected token `CREATE`, expected `;`
  |> ???:4:1
  |
1 | {
  | -... This last statement here was parsed as an expression
  | ...
3 | }
  |  - Expected a `;` here
4 | CREATE FOO;
  | ^^^^^^ Maybe missing a semicolon before this token?


*/