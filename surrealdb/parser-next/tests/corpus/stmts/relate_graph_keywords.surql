RELATE a:1->edge->sleep:b;
RELATE sleep:a->sleep->sleep:b;
RELATE sleep:b<-edge<-a:1;
RELATE a:1->edge->(SELECT * FROM b);
RELATE a:1->edge->SELECT * FROM b;
