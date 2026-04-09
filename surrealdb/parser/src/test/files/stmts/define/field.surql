DEFINE FIELD f ON t;
DEFINE FIELD f ON t TYPE { a: int };
DEFINE FIELD f ON t FLEXIBLE TYPE { a: int };
DEFINE FIELD f ON t READONLY FLEXIBLE;
DEFINE FIELD f ON t VALUE 1 READONLY;
DEFINE FIELD f ON t ASSERT 1 == 1 VALUE 1;
DEFINE FIELD f ON t COMPUTED 2 ASSERT 1 == 1;
DEFINE FIELD f ON t DEFAULT 3 COMPUTED 2;
DEFINE FIELD f ON t PERMISSIONS FULL DEFAULT 3;
DEFINE FIELD f ON t COMMENT "comment" PERMISSIONS FULL;
DEFINE FIELD f ON t REFERENCE ON DELETE IGNORE COMMENT "comment";
DEFINE FIELD f ON t REFERENCE ON DELETE REJECT;
DEFINE FIELD f ON t REFERENCE ON DELETE CASCADE;
DEFINE FIELD f ON t REFERENCE ON DELETE UNSET;
DEFINE FIELD f ON t REFERENCE ON DELETE THEN 1 - 2;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: Some
                    Type::Prime
                            -PrimeType::LitObject
                                LitObjectType
                                    .entries: Some
                                        -LitObjectTypeEntry
                                            .name: Ident
                                                .text: "a"
                                            .ty: Type::Prime
                                                    -PrimeType::Integer
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: Some
                    Type::Prime
                            -PrimeType::LitObject
                                LitObjectType
                                    .entries: Some
                                        -LitObjectTypeEntry
                                            .name: Ident
                                                .text: "a"
                                            .ty: Type::Prime
                                                    -PrimeType::Integer
                    .flexible: true
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: true
                    .readonly: true
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: true
                    .value: Some
                    Expr::Integer
                        +1
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: Some
                    Expr::Integer
                        +1
                    .assert: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .computed: Some
                    Expr::Integer
                        +2
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: Some
                    Expr::Integer
                        +2
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +3
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +3
                    .permissions: Some
                    FieldPermissions
                        .create: Some
                        Permission::Full
                        .update: Some
                        Permission::Full
                        .select: Some
                        Permission::Full
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: Some
                    FieldPermissions
                        .create: Some
                        Permission::Full
                        .update: Some
                        Permission::Full
                        .select: Some
                        Permission::Full
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "comment"
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "comment"
                    .on_delete: Some
                    OnDelete::Ignore
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: Some
                    OnDelete::Reject
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: Some
                    OnDelete::Cascade
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: Some
                    OnDelete::Unset
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "f"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "t"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: None
                    .comment: None
                    .on_delete: Some
                    OnDelete::Then
                        Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: Subtract
                                .right: Expr::Integer
                                    +2
*/