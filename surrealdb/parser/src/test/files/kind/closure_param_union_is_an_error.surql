RETURN |$x: any | int| $x;
/* ===== result =====
ERROR:Error: Unexpected token `|`, expected `;`
  |> ???:1:22
  |
1 | RETURN |$x: any | int| $x;
  |                      ^ Maybe missing a semicolon before this token?
1 | RETURN |$x: any | int| $x;
  | --------------------- This last statement here was parsed as an expression
1 | RETURN |$x: any | int| $x;
  |                      - Expected a `;` here


*/