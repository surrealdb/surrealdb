if true { 1 } else { 2 };
if true then  1  else  2  end;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::If
                If
                    .condition: Expr::Builtin
                        Builtin::True
                    .then: Expr::Block
                        Block
                            .exprs: Some
                                -Expr::Integer
                                    +1
                    .otherwise: Some
                        Expr::Block
                            Block
                                .exprs: Some
                                    -Expr::Integer
                                        +2
        -TopLevelExpr::Expr
            Expr::If
                If
                    .condition: Expr::Builtin
                        Builtin::True
                    .then: Expr::Integer
                        +1
                    .otherwise: Some
                        Expr::Integer
                            +2
*/