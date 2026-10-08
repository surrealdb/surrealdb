DEFINE FUNCTION fn::f($x: any | int) {};
/* ===== result =====
ERROR:Error: Unexpected token `|`
  |> ???:1:31
  |
1 | DEFINE FUNCTION fn::f($x: any | int) {};
  |                               ^ 
1 | DEFINE FUNCTION fn::f($x: any | int) {};
  |                           --- Cannot union kinds with kind `any` or `option<..>`


*/