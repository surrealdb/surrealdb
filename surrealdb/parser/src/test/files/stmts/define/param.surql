DEFINE PARAM $p;
DEFINE PARAM OVERWRITE $p;
DEFINE PARAM IF NOT EXISTS $p;
DEFINE PARAM $p COMMENT "a";
DEFINE PARAM $p VALUE 1 + 2;
DEFINE PARAM $p PERMISSIONS FULL VALUE 1 + 2;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineParam
                DefineParam
                    .kind: Create
                    .param: Param
                        .text: "p"
                    .value: None
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineParam
                DefineParam
                    .kind: Overwrite
                    .param: Param
                        .text: "p"
                    .value: None
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineParam
                DefineParam
                    .kind: IfNotExists
                    .param: Param
                        .text: "p"
                    .value: None
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineParam
                DefineParam
                    .kind: Create
                    .param: Param
                        .text: "p"
                    .value: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "a"
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineParam
                DefineParam
                    .kind: Create
                    .param: Param
                        .text: "p"
                    .value: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +2
                    .comment: None
                    .permission: None
        -TopLevelExpr::Expr
            Expr::DefineParam
                DefineParam
                    .kind: Create
                    .param: Param
                        .text: "p"
                    .value: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +2
                    .comment: None
                    .permission: Some
                    Permission::Full
*/