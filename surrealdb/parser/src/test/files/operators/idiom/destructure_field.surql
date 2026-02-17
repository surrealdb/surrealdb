a.{ field1, field2 };
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .op: PostfixOperator::Destructure
                        .op: Some
                            -Destructure
                                .field: Ident
                                    .text: "field1"
                                .op: None
                            -Destructure
                                .field: Ident
                                    .text: "field2"
                                .op: None
*/