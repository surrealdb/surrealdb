/**
[test]
reason = """
Setup for relation_degree.surql: the old binary writes a hub vertex with an
asymmetric fan-out — eight outbound edges and three inbound ones — plus an
isolated vertex with none at all.

Every edge id is explicit so the body can retract one named edge from each
direction and assert the exact degree that must remain. The fan-out is kept
well under the fold threshold, so the hub's adjacency stays in the per-edge
layout the writing binary produced and the new binary has to count that layout
rather than a repacked one."""
run = false
*/
DEFINE TABLE up_degree_person SCHEMALESS;
DEFINE TABLE up_degree_knows TYPE RELATION FROM up_degree_person TO up_degree_person SCHEMALESS;

-- The vertex every assertion in the body is anchored on.
CREATE up_degree_person:hub;

-- A vertex that is never related to anything: its degree must read as zero
-- rather than error on an adjacency scope the writing binary never created.
CREATE up_degree_person:lone;

-- The fan-out targets.
CREATE up_degree_person:t1;
CREATE up_degree_person:t2;
CREATE up_degree_person:t3;
CREATE up_degree_person:t4;
CREATE up_degree_person:t5;
CREATE up_degree_person:t6;
CREATE up_degree_person:t7;
CREATE up_degree_person:t8;

-- Eight outbound edges from the hub, each landing on a distinct target so a
-- neighbour walk and a degree count have the same cardinality.
RELATE up_degree_person:hub -> up_degree_knows:o1 -> up_degree_person:t1;
RELATE up_degree_person:hub -> up_degree_knows:o2 -> up_degree_person:t2;
RELATE up_degree_person:hub -> up_degree_knows:o3 -> up_degree_person:t3;
RELATE up_degree_person:hub -> up_degree_knows:o4 -> up_degree_person:t4;
RELATE up_degree_person:hub -> up_degree_knows:o5 -> up_degree_person:t5;
RELATE up_degree_person:hub -> up_degree_knows:o6 -> up_degree_person:t6;
RELATE up_degree_person:hub -> up_degree_knows:o7 -> up_degree_person:t7;
RELATE up_degree_person:hub -> up_degree_knows:o8 -> up_degree_person:t8;

-- Three inbound edges, so the two directions of the hub's adjacency hold
-- different counts and neither can stand in for the other.
RELATE up_degree_person:t1 -> up_degree_knows:i1 -> up_degree_person:hub;
RELATE up_degree_person:t2 -> up_degree_knows:i2 -> up_degree_person:hub;
RELATE up_degree_person:t3 -> up_degree_knows:i3 -> up_degree_person:hub;
