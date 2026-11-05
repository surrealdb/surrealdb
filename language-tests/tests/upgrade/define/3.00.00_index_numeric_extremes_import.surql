/**
[test]
reason = """
Setup for 3.00.00_index_numeric_extremes.surql and its _current sibling: the
old binary writes numbers at the edges of every kind into a standard index, a
UNIQUE index, two compound indexes and array record ids.

The edges are the values whose key bytes come from special cases rather than
from the general digit conversion: both zeros, NaN and the infinities, the
largest and smallest floats, the extremes of the integer and decimal ranges,
and decimals whose trailing zeros are part of their key."""
run = false
*/
DEFINE INDEX nx_v ON TABLE nx COLUMNS v;
CREATE nx:negzero SET v = -0.0f;
CREATE nx:zero SET v = 0;
CREATE nx:nan SET v = <float> "NaN";
CREATE nx:inf SET v = math::inf;
CREATE nx:neginf SET v = math::neg_inf;
CREATE nx:fmax SET v = 1.7976931348623157e308;
CREATE nx:fmin SET v = -1.7976931348623157e308;
CREATE nx:tiny SET v = 5e-324;
CREATE nx:negtiny SET v = -5e-324;
CREATE nx:dmax SET v = 79228162514264337593543950335dec;
CREATE nx:dmin SET v = -79228162514264337593543950335dec;
CREATE nx:dsmall SET v = 0.0000000000000000000000000001dec;
CREATE nx:imin SET v = -9223372036854775808;
CREATE nx:imax SET v = 9223372036854775807;
CREATE nx:one_int SET v = 1;
CREATE nx:one_float SET v = 1.0f;
CREATE nx:one_dec SET v = 1.0dec;
CREATE nx:one_dec_wide SET v = 1.000dec;
CREATE nx:third SET v = 0.3333333333333333f;
CREATE nx:third_dec SET v = 0.3333333333333333dec;
CREATE nx:tenth SET v = 0.1f;
CREATE nx:big SET v = 123456789012345678901234567890.123456789f;
DEFINE INDEX nxu_v ON TABLE nxu COLUMNS v UNIQUE;
CREATE nxu:negzero SET v = -0.0f;
CREATE nxu:nan SET v = <float> "NaN";
CREATE nxu:inf SET v = math::inf;
CREATE nxu:neginf SET v = math::neg_inf;
CREATE nxu:fmax SET v = 1.7976931348623157e308;
CREATE nxu:tiny SET v = 5e-324;
CREATE nxu:dmax SET v = 79228162514264337593543950335dec;
CREATE nxu:imin SET v = -9223372036854775808;
CREATE nxu:one_dec SET v = 1.0dec;
CREATE nxu:tenth SET v = 0.1f;
DEFINE INDEX nxc_ab ON TABLE nxc COLUMNS a, b;
CREATE nxc:1 SET a = 1, b = 0.5f;
CREATE nxc:2 SET a = 1, b = -0.5f;
CREATE nxc:3 SET a = 1, b = 2.50dec;
CREATE nxc:4 SET a = -1, b = 3;
CREATE nxc:5 SET a = 2.5f, b = 0.1f;
CREATE nxc:6 SET a = 0.0f, b = 1e300;
CREATE nxc:7 SET a = -0.0f, b = 1e-300;
CREATE nxc:8 SET a = 1e19, b = 7;
DEFINE INDEX nxcu_ab ON TABLE nxcu COLUMNS a, b UNIQUE;
CREATE nxcu:1 SET a = 1.5dec, b = 2;
CREATE nxcu:2 SET a = 1.5dec, b = 3;
CREATE nxcu:3 SET a = -2.25f, b = 0.1f;
CREATE nxcu:4 SET a = 3, b = 3.486844328132695e67;
CREATE nxcu:5 SET a = 3, b = -2.580693254293312e65;
CREATE nxr:[1.5f, 2dec] SET n = 1;
CREATE nxr:[-1.5f, 2dec] SET n = 2;
CREATE nxr:[1.5f, 3] SET n = 3;
CREATE nxr:[3, 0.1f] SET n = 4;
CREATE nxr:[100000000000000000000dec, 1] SET n = 5;
CREATE nxr:[0.25f, 1e-300] SET n = 6;
CREATE nxr:7 SET n = 7;
CREATE nxr:-7 SET n = 8;
