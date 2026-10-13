FOR $i IN 0..10{
	DEFINE FUNCTION fn::foo(){ BREAK }
}
/* ===== result =====
ERROR:Error: Invalid use of control flow statement outside of loop body
  |> ???:2:32
  |
2 |     DEFINE FUNCTION fn::foo(){ BREAK }
  |                                ^^^^^ 


*/