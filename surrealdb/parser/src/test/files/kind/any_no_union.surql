<any | int> 1;
/* ===== result =====
ERROR:Error: Unexpected token `|`
  |> ???:1:6
  |
1 | <any | int> 1;
  |      ^ 
1 | <any | int> 1;
  |  ^^^ Cannot union kinds with kind `any` or `option<..>`


*/