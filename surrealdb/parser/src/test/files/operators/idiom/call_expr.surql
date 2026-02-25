{ a: 1 }();
{ a: 1 }.a();
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Object
                        Object
                            .entries: Some
                                -ObjectEntry
                                    .key: "a"
                                    .value: Expr::Integer
                                        +1
                    .op: PostfixOperator::Call
                        .args: None
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Object
                                Object
                                    .entries: Some
                                        -ObjectEntry
                                            .key: "a"
                                            .value: Expr::Integer
                                                +1
                            .op: PostfixOperator::Field
                                .name: "a"
                    .op: PostfixOperator::Call
                        .args: None
*/