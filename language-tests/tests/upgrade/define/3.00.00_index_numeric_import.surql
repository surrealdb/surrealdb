/**
[test]
reason = """
Setup for 3.00.00_index_numeric.surql and 3.00.00_index_numeric_current.surql:
the old binary encodes integer, float and decimal values into a standard and a
unique index.

The floats include values whose key digits are not the exactly rounded decimal
expansion: 3.486844328132695e67, -2.580693254293312e65, 3.244745644034682e-48
and 4.578586377376824e-300 sit where the encoder's guarded correction for a
truncated power of two rounds the last digit the other way. A reader that
computes those digits differently cannot find their keys."""
run = false
*/
DEFINE INDEX idx_num_v ON TABLE idx_num COLUMNS v;
CREATE idx_num:1 SET v = -1000;
CREATE idx_num:2 SET v = -2.5f;
CREATE idx_num:3 SET v = -0.5dec;
CREATE idx_num:4 SET v = 0;
CREATE idx_num:5 SET v = 0.1f;
CREATE idx_num:6 SET v = 1.50dec;
CREATE idx_num:7 SET v = 3;
CREATE idx_num:8 SET v = 1000;
CREATE idx_num:9 SET v = 4.578586377376824e-300;
CREATE idx_num:10 SET v = 3.244745644034682e-48;
CREATE idx_num:11 SET v = 3.486844328132695e67;
CREATE idx_num:12 SET v = -2.580693254293312e65;
CREATE idx_num:13 SET v = 1e300;
CREATE idx_num:14 SET v = 9223372036854775807;
CREATE idx_num:15 SET v = 0.0000001421817112187826738850dec;
DEFINE INDEX idx_numu_v ON TABLE idx_numu COLUMNS v UNIQUE;
CREATE idx_numu:1 SET v = 3.486844328132695e67;
CREATE idx_numu:2 SET v = 3.244745644034682e-48;
CREATE idx_numu:3 SET v = -2.580693254293312e65;
CREATE idx_numu:4 SET v = 0.1f;
CREATE idx_numu:5 SET v = 1.50dec;
CREATE idx_numu:6 SET v = 7;
CREATE idx_numu:7 SET v = -9223372036854775807;
