DEFINE FIELD f ON t PERMISSIONS FOR select NONE FOR update NONE;
DEFINE FIELD f ON t PERMISSIONS FOR create, update WHERE true FOR select FULL;
DEFINE FIELD f ON t PERMISSIONS FOR select FULL, FOR update NONE;
DEFINE FIELD f ON t PERMISSIONS FOR select WHERE true FOR create WHERE false;
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
                    .inline: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: Some
                    FieldPermissions
                        .create: None
                        .update: Some
                        Permission::None
                        .select: Some
                        Permission::None
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
                    .inline: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: Some
                    FieldPermissions
                        .create: Some
                        Permission::Where
                            Expr::Builtin
                                Builtin::True
                        .update: Some
                        Permission::Where
                            Expr::Builtin
                                Builtin::True
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
                    .inline: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: Some
                    FieldPermissions
                        .create: None
                        .update: Some
                        Permission::None
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
                    .inline: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: None
                    .permissions: Some
                    FieldPermissions
                        .create: Some
                        Permission::Where
                            Expr::Builtin
                                Builtin::False
                        .update: None
                        .select: Some
                        Permission::Where
                            Expr::Builtin
                                Builtin::True
                    .comment: None
                    .on_delete: None
*/