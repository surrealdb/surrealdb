if 1 { 2 } else if 3 { 4 };
if 1 THEN 2 ELSE IF 3 THEN 4 END;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::If
                If
                    .condition: Expr::Integer
                        +1
                    .then: Expr::Block
                        Block
                            .exprs: Some
                                -Expr::Integer
                                    +2
                    .otherwise: Some
                        Expr::If
                            If
                                .condition: Expr::Integer
                                    +3
                                .then: Expr::Block
                                    Block
                                        .exprs: Some
                                            -Expr::Integer
                                                +4
                                .otherwise: None
        -TopLevelExpr::Expr
            Expr::If
                If
                    .condition: Expr::Integer
                        +1
                    .then: Expr::Integer
                        +2
                    .otherwise: Some
                        Expr::If
                            If
                                .condition: Expr::Integer
                                    +3
                                .then: Expr::Integer
                                    +4
                                .otherwise: None
*/