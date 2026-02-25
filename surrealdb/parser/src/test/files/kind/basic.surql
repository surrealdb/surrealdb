<any> 1;
<NONE> 1;
<NULL> 1;
<true> 1;
<false> 1;
<bool> 1;
<NaN> 1;
<Infinity> 1;
< -Infinity> 1;
<int> 1;
<float> 1;
<decimal> 1;
<number> 1;
<object> 1;
<duration> 1;
<datetime> 1;
<bytes> 1;
<regex> 1;
<uuid> 1;
<string> 1;
<function> 1;
<record> 1;
<table> 1;
<array> 1;
<set> 1;
<geometry> 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Any
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitBuiltin
                                Builtin::None
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitBuiltin
                                Builtin::Null
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitBuiltin
                                Builtin::True
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitBuiltin
                                Builtin::False
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Bool
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitFloat
                                NaN
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitFloat
                                inf
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::LitFloat
                                -inf
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Integer
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Float
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Decimal
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Number
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Object
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Duration
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::DateTime
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Bytes
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Regex
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Uuid
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::String
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Function
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Record
                                IdentListType
                                    .idents: None
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Table
                                IdentListType
                                    .idents: None
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
                            -PrimeType::Set
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
                            -PrimeType::Geometry
                                GeometryType
                                    .types: None
                    .right: Expr::Integer
                        +1
*/