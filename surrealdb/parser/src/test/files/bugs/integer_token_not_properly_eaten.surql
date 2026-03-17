create test:100000000000000000000;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Create
                Create
                    .only: false
                    .targets: 
                        -Expr::RecordId
                            RecordId
                                .name: Ident
                                    .text: "test"
                                .key: RecordIdKey::String
                                    StringLit
                                        .text: "100000000000000000000"
                    .data: None
                    .output: None
                    .version: None
                    .timeout: None
*/