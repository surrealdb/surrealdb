vim.lsp.config("rust_analyzer", {
	settings = {
		["rust-analyzer"] = {
			cargo = {
				features = { "jwks" }
			},
			check = {
				command = "clippy",
				extraArgs = { "--lib" },
			},
		},
	},
})
vim.lsp.enable("rust_analyzer")
