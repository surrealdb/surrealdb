a.{ field1: true, field2: 1 + 1 };
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
                                .op: Some
                                    DestructureOperator::Expr
                                        .expr: Expr::Builtin
                                            Builtin::True
                            -Destructure
                                .field: Ident
                                    .text: "field2"
                                .op: Some
                                    DestructureOperator::Expr
                                        .expr: Expr::Binary
                                            BinaryExpr
                                                .left: Expr::Integer
                                                    +1
                                                .op: Add
                                                .right: Expr::Integer
                                                    +1
*/