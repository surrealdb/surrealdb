DEFINE FUNCTION fn::a::b(){};
DEFINE FUNCTION OVERWRITE fn::a::b(){};
DEFINE FUNCTION IF NOT EXISTS fn::a::b(){};
DEFINE FUNCTION fn::a::b($c: int, $d:int){};
DEFINE FUNCTION fn::a::b($c: int, $d:int) -> int{};
DEFINE FUNCTION fn::a::b($c: int, $d:int) -> int{} COMMENT "com";
DEFINE FUNCTION fn::a::b($c: int, $d:int) -> int{} COMMENT "com";
DEFINE FUNCTION fn::a::b($c: int, $d:int) -> int{} PERMISSIONS FULL COMMENT "com";
DEFINE FUNCTION fn::a::b($c: int, $d:int) -> int{} PERMISSIONS NONE COMMENT "com";
DEFINE FUNCTION fn::a::b($c: int, $d:int) -> int{} PERMISSIONS WHERE $session = "bla" COMMENT "com";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: None
                    .return_ty: None
                    .body: Block
                        .exprs: None
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Overwrite
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: None
                    .return_ty: None
                    .body: Block
                        .exprs: None
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: IfNotExists
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: None
                    .return_ty: None
                    .body: Block
                        .exprs: None
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: Some
                        -Parameter
                            .name: Param
                                .text: "c"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                        -Parameter
                            .name: Param
                                .text: "d"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                    .return_ty: None
                    .body: Block
                        .exprs: None
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: Some
                        -Parameter
                            .name: Param
                                .text: "c"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                        -Parameter
                            .name: Param
                                .text: "d"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                    .return_ty: Some
                    Type::Prime
                            -PrimeType::Integer
                    .body: Block
                        .exprs: None
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: Some
                        -Parameter
                            .name: Param
                                .text: "c"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                        -Parameter
                            .name: Param
                                .text: "d"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                    .return_ty: Some
                    Type::Prime
                            -PrimeType::Integer
                    .body: Block
                        .exprs: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "com"
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: Some
                        -Parameter
                            .name: Param
                                .text: "c"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                        -Parameter
                            .name: Param
                                .text: "d"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                    .return_ty: Some
                    Type::Prime
                            -PrimeType::Integer
                    .body: Block
                        .exprs: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "com"
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: Some
                        -Parameter
                            .name: Param
                                .text: "c"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                        -Parameter
                            .name: Param
                                .text: "d"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                    .return_ty: Some
                    Type::Prime
                            -PrimeType::Integer
                    .body: Block
                        .exprs: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "com"
                    .permission: Some
                    Permission::Full
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: Some
                        -Parameter
                            .name: Param
                                .text: "c"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                        -Parameter
                            .name: Param
                                .text: "d"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                    .return_ty: Some
                    Type::Prime
                            -PrimeType::Integer
                    .body: Block
                        .exprs: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "com"
                    .permission: Some
                    Permission::None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .parameters: Some
                        -Parameter
                            .name: Param
                                .text: "c"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                        -Parameter
                            .name: Param
                                .text: "d"
                            .ty: Some
                            Type::Prime
                                    -PrimeType::Integer
                    .return_ty: Some
                    Type::Prime
                            -PrimeType::Integer
                    .body: Block
                        .exprs: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "com"
                    .permission: Some
                    Permission::Where
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Param
                                    Param
                                        .text: "session"
                                .op: Equal
                                .right: Expr::String
                                    StringLit
                                        .text: "bla"
*/