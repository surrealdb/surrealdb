SELECT ->knows:rand()->person FROM person:1;
/* ===== result =====
ERROR:Error: Unexpected token `->`, expected a record id key range
  |> ???:1:22
  |
1 | SELECT ->knows:rand()->person FROM person:1;
  |                      ^^ 


*/