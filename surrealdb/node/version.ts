/**
 * The version `@surrealdb/node-native` carries.
 *
 * Not committed: it is the engine version, taken from `SURREAL_VERSION` when
 * the release workflow supplies one and otherwise read out of the Cargo
 * workspace — which is where the release workflow reads it from too, at the
 * same commit. That keeps the addon's version identical to the `surrealdb`
 * release whose engine it embeds, with nothing to bump by hand.
 *
 * Both `build.ts` and `publish.ts` resolve it through here, because they must
 * agree: the loader `napi build` generates bakes in the version it expects the
 * per-platform binary package to carry, and `publish.ts` is what stamps that
 * package.
 */
export async function packageVersion(): Promise<string> {
	if (process.env.SURREAL_VERSION) {
		return process.env.SURREAL_VERSION;
	}

	const manifest = await Bun.file(`${import.meta.dir}/../../Cargo.toml`).text();
	const section = manifest.split(/^\[workspace\.package\]$/m)[1];
	const version = section?.match(/^version\s*=\s*"([^"]+)"/m)?.[1];

	if (!version) {
		console.error("❌ Could not read [workspace.package] version from ../../Cargo.toml");
		process.exit(1);
	}

	return version;
}
