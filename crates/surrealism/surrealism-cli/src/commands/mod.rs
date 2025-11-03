pub mod build;
pub mod info;
pub mod run;
pub mod sig;

pub trait SurrealismCommand {
	async fn run(self) -> anyhow::Result<()>;
}
