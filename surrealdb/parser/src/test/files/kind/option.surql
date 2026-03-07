<option<int | float>> 1;
<option<[1,2,3]>> 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::None
                            -PrimeType::Integer
                            -PrimeType::Float
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::None
                            -PrimeType::LitArray
                                LitArrayType
                                    .entries: Some
                                        -Type::Prime
                                            -PrimeType::LitInteger
                                                +1
                                        -Type::Prime
                                            -PrimeType::LitInteger
                                                +2
                                        -Type::Prime
                                            -PrimeType::LitInteger
                                                +3
                    .right: Expr::Integer
                        +1
*/