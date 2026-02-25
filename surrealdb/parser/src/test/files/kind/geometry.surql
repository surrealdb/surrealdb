<geometry<point>> 1;
<geometry<line>> 1;
<geometry<polygon>> 1;
<geometry<multiline>> 1;
<geometry<multipolygon>> 1;
<geometry<collection>> 1;
<geometry<point | line |polygon| multiline| multipolygon|collection>> 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Geometry
                                GeometryType
                                    .types: Some
                                        -GeometrySubType::Point
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Geometry
                                GeometryType
                                    .types: Some
                                        -GeometrySubType::Line
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Geometry
                                GeometryType
                                    .types: Some
                                        -GeometrySubType::Polygon
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Geometry
                                GeometryType
                                    .types: Some
                                        -GeometrySubType::MultiLine
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Geometry
                                GeometryType
                                    .types: Some
                                        -GeometrySubType::MultiPolygon
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Geometry
                                GeometryType
                                    .types: Some
                                        -GeometrySubType::Collection
                    .right: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Prefix
                PrefixExpr
                    .op: PrefixOperator::Cast
                        Type::Prime
                            -PrimeType::Geometry
                                GeometryType
                                    .types: Some
                                        -GeometrySubType::Point
                                        -GeometrySubType::Line
                                        -GeometrySubType::Polygon
                                        -GeometrySubType::MultiLine
                                        -GeometrySubType::MultiPolygon
                                        -GeometrySubType::Collection
                    .right: Expr::Integer
                        +1
*/