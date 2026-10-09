RELATE (type::record('person', $i))->knows->(type::record('person', $i * 3));
RELATE (type::record('person', $i))<-knows<-(type::record('person', $i * 3));
RELATE (a:1)->(knows)->(b:2);
RELATE (a:1)->knows->b:2;
RELATE b:2<-knows<-(a:1);
