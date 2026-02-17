vim.lsp.config('rust_analyzer', {
	settings = {
		['rust-analyzer'] = {
			cargo = {
				features = { "visualize" }
			},
			check = {
				command = "clippy"
			},
			diagnostics = {
				disabled = { "inactive-code" }
			}
		}
	}
})
