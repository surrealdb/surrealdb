+1;
++1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Positive
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Positive
                    .right: Expr::Prefix
                        PrefixExpr
                            .op: PrefixOperator::Positive
                            .right: Expr::Integer
                                +1
*/