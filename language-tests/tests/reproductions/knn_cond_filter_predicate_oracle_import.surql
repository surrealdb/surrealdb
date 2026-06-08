/**
[test]
reason = "Setup: an HNSW-indexed table where two of the three rows are hidden from a record user. The probe vector [10f] is nearest to the two hidden rows (pts:2 / pts:3). The fixture is used to assert that varying a residual WHERE on the indexed table does not let a record user infer hidden field values via the ANN cond pre-filter."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM user WHERE id = $id );

CREATE user:alice;

DEFINE TABLE pts SCHEMAFULL PERMISSIONS FOR SELECT WHERE public = true;
DEFINE FIELD point ON pts TYPE array<float>;
DEFINE FIELD public ON pts TYPE bool;
DEFINE FIELD secret ON pts TYPE string;
DEFINE INDEX hn_pt ON pts FIELDS point HNSW DIMENSION 1;

INSERT INTO pts [
    { id: pts:1, point: [100f], public: true,  secret: "A" },
    { id: pts:2, point: [10f],  public: false, secret: "A" },
    { id: pts:3, point: [11f],  public: false, secret: "B" }
];
