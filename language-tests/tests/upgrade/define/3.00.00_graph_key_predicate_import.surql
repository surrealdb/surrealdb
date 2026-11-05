/**
[test]
reason = "Setup for graph_key_predicate.surql: the old binary lays down a hub vertex with three outbound and two inbound edges, each carrying an explicit edge id and a numeric field, so the reader can filter on either endpoint, on the edge id, or on a non-key field."
run = false
*/
DEFINE TABLE up_kpred_person SCHEMALESS;
DEFINE TABLE up_kpred_knows TYPE RELATION FROM up_kpred_person TO up_kpred_person SCHEMALESS;
-- A non-key field, so one assertion can force the reader off the key bytes and
-- onto the edge record.
DEFINE FIELD score ON up_kpred_knows TYPE number;

CREATE up_kpred_person:hub;
CREATE up_kpred_person:t1;
CREATE up_kpred_person:t2;
CREATE up_kpred_person:t3;
CREATE up_kpred_person:t4;

-- Outbound edges. Explicit ids so a predicate can name one exactly.
RELATE up_kpred_person:hub -> up_kpred_knows:o1 -> up_kpred_person:t1 SET score = 3;
RELATE up_kpred_person:hub -> up_kpred_knows:o2 -> up_kpred_person:t2 SET score = 7;
RELATE up_kpred_person:hub -> up_kpred_knows:o3 -> up_kpred_person:t3 SET score = 9;

-- Inbound edges, so the same predicates can be asked in the other direction and
-- an in/out orientation swap is observable.
RELATE up_kpred_person:t2 -> up_kpred_knows:i1 -> up_kpred_person:hub SET score = 4;
RELATE up_kpred_person:t4 -> up_kpred_knows:i2 -> up_kpred_person:hub SET score = 8;
