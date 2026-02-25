array::join([1,2], bla);
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "array"
                            .parts: Some
                                -PathSegment::Ident
                                    Ident
                                        .text: "join"
                    .op: PostfixOperator::Call
                        .args: Some
                            -Expr::Array
                                Array
                                    .entries: Some
                                        -Expr::Integer
                                            +1
                                        -Expr::Integer
                                            +2
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bla"
                                    .parts: None
*/