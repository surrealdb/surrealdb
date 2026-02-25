<{ a: 1}> 1;
<{ "a": 1}> 1;
<{ `a`: 1}> 1;
<{ SELECT: 1}> 1;
<{ a: { b: 1 } | int, c: float }> 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitObject
                                LitObjectType
                                    .entries: Some
                                        -LitObjectTypeEntry
                                            .name: Ident
                                                .text: "a"
                                            .ty: Type::Prime
                                                -PrimeType::LitInteger
                                                    +1
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitObject
                                LitObjectType
                                    .entries: Some
                                        -LitObjectTypeEntry
                                            .name: Ident
                                                .text: "a"
                                            .ty: Type::Prime
                                                -PrimeType::LitInteger
                                                    +1
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitObject
                                LitObjectType
                                    .entries: Some
                                        -LitObjectTypeEntry
                                            .name: Ident
                                                .text: "a"
                                            .ty: Type::Prime
                                                -PrimeType::LitInteger
                                                    +1
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitObject
                                LitObjectType
                                    .entries: Some
                                        -LitObjectTypeEntry
                                            .name: Ident
                                                .text: "SELECT"
                                            .ty: Type::Prime
                                                -PrimeType::LitInteger
                                                    +1
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitObject
                                LitObjectType
                                    .entries: Some
                                        -LitObjectTypeEntry
                                            .name: Ident
                                                .text: "a"
                                            .ty: Type::Prime
                                                -PrimeType::LitObject
                                                    LitObjectType
                                                        .entries: Some
                                                            -LitObjectTypeEntry
                                                                .name: Ident
                                                                    .text: "b"
                                                                .ty: Type::Prime
                                                                    -PrimeType::LitInteger
                                                                        +1
                                                -PrimeType::Integer
                                        -LitObjectTypeEntry
                                            .name: Ident
                                                .text: "c"
                                            .ty: Type::Prime
                                                -PrimeType::Float
                    .right: Expr::Integer
                        +1
*/