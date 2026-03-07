{};
{ a: 1 };
{ "a": 1 };
{ "a": 1, "b" : 2, };
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Object
                Object
                    .entries: None
        -TopLevelExpr::Expr
            Expr::Object
                Object
                    .entries: Some
                        -ObjectEntry
                            .key: "a"
                            .value: Expr::Integer
                                +1
        -TopLevelExpr::Expr
            Expr::Object
                Object
                    .entries: Some
                        -ObjectEntry
                            .key: "a"
                            .value: Expr::Integer
                                +1
        -TopLevelExpr::Expr
            Expr::Object
                Object
                    .entries: Some
                        -ObjectEntry
                            .key: "a"
                            .value: Expr::Integer
                                +1
                        -ObjectEntry
                            .key: "b"
                            .value: Expr::Integer
                                +2
*/