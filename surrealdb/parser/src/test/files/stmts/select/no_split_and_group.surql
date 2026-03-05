SELECT a FROM b SPLIT ON a GrOUP BY b;
/* ===== result =====
ERROR:Error: Unexpected token `GrOUP`, selects cannot both have a `GROUP BY` clause and a `SPLIT ON` clause
  |> ???:1:28
  |
1 | SELECT a FROM b SPLIT ON a GrOUP BY b;
  |                            ^^^^^ 
1 | SELECT a FROM b SPLIT ON a GrOUP BY b;
  |                 ---------- Previous `SPLIT ON` clause


*/