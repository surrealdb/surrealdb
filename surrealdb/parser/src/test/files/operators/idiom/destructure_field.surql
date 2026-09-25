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
                    .op: IdiomOperator::Destructure
                        Some
                            -DestructureItem::Field
                                Destructure
                                    .field: Ident
                                        .text: "field1"
                                    .op: None
                            -DestructureItem::Field
                                Destructure
                                    .field: Ident
                                        .text: "field2"
                                    .op: None
*/