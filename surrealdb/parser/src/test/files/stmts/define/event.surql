DEFINE EVENT a ON b WHEN a == 1 THEN (CREATE a:1), 2 + 3;
DEFINE EVENT a ON b WHEN a == 1 THEN (CREATE a:1), 2 + 3 COMMENT "foo";
DEFINE EVENT a ON b WHEN a == 1 THEN (CREATE a:1), 2 + 3 ASYNC;
DEFINE EVENT a ON b WHEN a == 1 THEN (CREATE a:1), 2 + 3 ASYNC RETRY 10 MAXDEPTH 10;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineEvent
                DefineEvent
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .then: 
                        -Expr::Covered
                            Expr::Create
                                Create
                                    .only: false
                                    .targets: 
                                        -Expr::RecordId
                                            RecordId
                                                .name: Ident
                                                    .text: "a"
                                                .key: RecordIdKey::Number
                                                    1
                                    .data: None
                                    .output: None
                                    .version: None
                                    .timeout: None
                        -Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +2
                                .op: Add
                                .right: Expr::Integer
                                    +3
                    .comment: None
                    .async_: None
        -TopLevelExpr::Expr
            Expr::DefineEvent
                DefineEvent
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .then: 
                        -Expr::Covered
                            Expr::Create
                                Create
                                    .only: false
                                    .targets: 
                                        -Expr::RecordId
                                            RecordId
                                                .name: Ident
                                                    .text: "a"
                                                .key: RecordIdKey::Number
                                                    1
                                    .data: None
                                    .output: None
                                    .version: None
                                    .timeout: None
                        -Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +2
                                .op: Add
                                .right: Expr::Integer
                                    +3
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "foo"
                    .async_: None
        -TopLevelExpr::Expr
            Expr::DefineEvent
                DefineEvent
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .then: 
                        -Expr::Covered
                            Expr::Create
                                Create
                                    .only: false
                                    .targets: 
                                        -Expr::RecordId
                                            RecordId
                                                .name: Ident
                                                    .text: "a"
                                                .key: RecordIdKey::Number
                                                    1
                                    .data: None
                                    .output: None
                                    .version: None
                                    .timeout: None
                        -Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +2
                                .op: Add
                                .right: Expr::Integer
                                    +3
                    .comment: None
                    .async_: Some
                    DefineEventAsync
                        .retry: None
                        .max_depth: None
        -TopLevelExpr::Expr
            Expr::DefineEvent
                DefineEvent
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .then: 
                        -Expr::Covered
                            Expr::Create
                                Create
                                    .only: false
                                    .targets: 
                                        -Expr::RecordId
                                            RecordId
                                                .name: Ident
                                                    .text: "a"
                                                .key: RecordIdKey::Number
                                                    1
                                    .data: None
                                    .output: None
                                    .version: None
                                    .timeout: None
                        -Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +2
                                .op: Add
                                .right: Expr::Integer
                                    +3
                    .comment: None
                    .async_: Some
                    DefineEventAsync
                        .retry: Some
                        +10
                        .max_depth: Some
                        +10
*/