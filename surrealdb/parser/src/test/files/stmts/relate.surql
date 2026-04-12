RELATE a->b->c;
RELATE c<-b<-a;
RELATE c<-b:d<-a;
RELATE c<-$param<-a;
RELATE ONLY c<-$param<-a MERGE { a: b };
RELATE ONLY c<-$param<-a MERGE { a: b } RETURN BEFORE;
RELATE ONLY c<-$param<-a MERGE { a: b } RETURN BEFORE TIMEOUT 1s;
/* ===== result =====
ERROR:Error: Unexpected token `->`, expected `:`
  |> ???:1:9
  |
1 | RELATE a->b->c;
  |         ^^ 


*/