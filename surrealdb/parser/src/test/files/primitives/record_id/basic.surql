a:1;
a:"b";
a:c;
a:['d'];
a:{ e: 2 };

/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Number
                        +1
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "b"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "c"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Array
                        Array
                            .entries: Some
                                -Expr::String
                                    StringLit
                                        .text: "d"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Object
                        Object
                            .entries: Some
                                -ObjectEntry
                                    .key: "e"
                                    .value: Expr::Integer
                                        +2
*/