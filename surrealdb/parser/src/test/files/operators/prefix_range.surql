..1;
..=1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Range
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::RangeInclusive
                    .right: Expr::Integer
                        +1
*/