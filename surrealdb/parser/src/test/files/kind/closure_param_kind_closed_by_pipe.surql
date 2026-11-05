RETURN |$x: any| $x;
RETURN |$x: option<int>| $x;
RETURN |$x: int, $y: any| $x;
RETURN |$x: <int | string>| $x;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Return
                Return
                    .expr: Expr::Closure
                        Closure
                            .parameters: Some
                                -Parameter
                                    .name: Param
                                        .text: "x"
                                    .ty: Some
                                    Type::Any
                            .output_ty: None
                            .body: Expr::Param
                                Param
                                    .text: "x"
                    .fetch: None
        -TopLevelExpr::Expr
            Expr::Return
                Return
                    .expr: Expr::Closure
                        Closure
                            .parameters: Some
                                -Parameter
                                    .name: Param
                                        .text: "x"
                                    .ty: Some
                                    Type::Prime
                                            -PrimeType::None
                                            -PrimeType::Integer
                            .output_ty: None
                            .body: Expr::Param
                                Param
                                    .text: "x"
                    .fetch: None
        -TopLevelExpr::Expr
            Expr::Return
                Return
                    .expr: Expr::Closure
                        Closure
                            .parameters: Some
                                -Parameter
                                    .name: Param
                                        .text: "x"
                                    .ty: Some
                                    Type::Prime
                                            -PrimeType::Integer
                                -Parameter
                                    .name: Param
                                        .text: "y"
                                    .ty: Some
                                    Type::Any
                            .output_ty: None
                            .body: Expr::Param
                                Param
                                    .text: "x"
                    .fetch: None
        -TopLevelExpr::Expr
            Expr::Return
                Return
                    .expr: Expr::Closure
                        Closure
                            .parameters: Some
                                -Parameter
                                    .name: Param
                                        .text: "x"
                                    .ty: Some
                                    Type::Prime
                                            -PrimeType::Integer
                                            -PrimeType::String
                            .output_ty: None
                            .body: Expr::Param
                                Param
                                    .text: "x"
                    .fetch: None
*/