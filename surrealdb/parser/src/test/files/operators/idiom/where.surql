a[? true];
a[where false];
a[WHERE 1 + 1];
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
                    .op: PostfixOperator::Where
                        .expr: Expr::Builtin
                            Builtin::True
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .op: PostfixOperator::Where
                        .expr: Expr::Builtin
                            Builtin::False
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .op: PostfixOperator::Where
                        .expr: Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: Add
                                .right: Expr::Integer
                                    +1
*/