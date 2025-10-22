
CREATE a SET v = 1, t = 1;
CREATE a SET v = 2, t = 1;

SELECT math::mean(v + 1) FROM a GROUP ALL;
SELECT math::mean(v) + 1 FROM a GROUP ALL;
