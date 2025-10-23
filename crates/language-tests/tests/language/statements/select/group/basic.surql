
CREATE a:1 SET v = 1, t = 1;
CREATE a:2 SET v = 2, t = 1;
CREATE a:3 SET v = 2, t = 2;
CREATE a:4 SET v = 3, t = 2;

SELECT math::mean(v + 1), t FROM a GROUP t;
SELECT math::mean(v) + 1, t FROM a GROUP t;
