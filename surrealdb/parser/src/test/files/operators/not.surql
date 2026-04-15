!1;
!!true;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Not
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Not
                    .right: Expr::Prefix
                        PrefixExpr
                            .op: PrefixOperator::Not
                            .right: Expr::Builtin
                                Builtin::True
*/