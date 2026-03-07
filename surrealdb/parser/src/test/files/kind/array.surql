<[1,2]> 1;
<[1,]> 1;
<[1,int]> 1;
<[1,[float,geometry]]> 1;
<array> 1;
<array<int>> 1;
<array<int,10>> 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitArray
                                LitArrayType
                                    .entries: Some
                                        -Type::Prime
                                            -PrimeType::LitInteger
                                                +1
                                        -Type::Prime
                                            -PrimeType::LitInteger
                                                +2
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitArray
                                LitArrayType
                                    .entries: Some
                                        -Type::Prime
                                            -PrimeType::LitInteger
                                                +1
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitArray
                                LitArrayType
                                    .entries: Some
                                        -Type::Prime
                                            -PrimeType::LitInteger
                                                +1
                                        -Type::Prime
                                            -PrimeType::Integer
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitArray
                                LitArrayType
                                    .entries: Some
                                        -Type::Prime
                                            -PrimeType::LitInteger
                                                +1
                                        -Type::Prime
                                            -PrimeType::LitArray
                                                LitArrayType
                                                    .entries: Some
                                                        -Type::Prime
                                                            -PrimeType::Float
                                                        -Type::Prime
                                                            -PrimeType::Geometry
                                                                GeometryType
                                                                    .types: None
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Array
                                ArrayLikeType
                                    .ty: None
                                    .size: None
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Array
                                ArrayLikeType
                                    .ty: Some
                                        Type::Prime
                                            -PrimeType::Integer
                                    .size: None
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Array
                                ArrayLikeType
                                    .ty: Some
                                        Type::Prime
                                            -PrimeType::Integer
                                    .size: Some
                                        +10
                    .right: Expr::Integer
                        +1
*/