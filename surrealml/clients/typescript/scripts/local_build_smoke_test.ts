import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import * as fs from 'fs';
import * as path from 'path';
import { spawnSync } from 'child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function parseArgs(): boolean {
	const args = process.argv.slice(2);
	let buildLocal = true;
	for (const arg of args) {
		if (arg === '--no-build-local') buildLocal = false;
		if (arg === '--build-local')    buildLocal = true;
	}
	return buildLocal;
}

async function main() {
	const buildLocal = parseArgs();

	// paths
	const scriptDir   = __dirname;
	const tsClientDir = path.join(scriptDir, '..');
	const sandboxDir  = path.join(tsClientDir, 'sandbox');
	const repoRoot    = path.join(tsClientDir, '..', '..');
	const stashDir    = path.join(repoRoot, 'modules', 'core', 'model_stash', 'onnx');

	// sandbox reset
	if (fs.existsSync(sandboxDir)) {
		console.log(`🧹 Removing existing sandbox at ${sandboxDir}`);
		fs.rmSync(sandboxDir, { recursive: true, force: true });
	};
	fs.mkdirSync(sandboxDir, { recursive: true });

	// clean out old build
	const distDir = path.join(tsClientDir, 'dist');
		if (fs.existsSync(distDir)) {
		console.log(`🧹 Removing old build at ${distDir}`);
		fs.rmSync(distDir, { recursive: true, force: true });
	};
	
	// minimal sandbox package.json / tsconfig
	fs.writeFileSync(
		path.join(sandboxDir, 'package.json'),
		JSON.stringify({ name: 'sml-sandbox', private: true, type: 'module', devDependencies: { tsx: '^4.19.4' } }, null, 2)
	);
	fs.writeFileSync(
		path.join(sandboxDir, 'tsconfig.json'),
		JSON.stringify({ compilerOptions: { module: 'NodeNext', target: 'ES2022', strict: true, esModuleInterop: true, resolveJsonModule: true } }, null, 2)
	);

	// build the client
	console.log('🔨 Building local TS client…');
	let r = spawnSync('npm', ['run', 'build'], { cwd: tsClientDir, stdio: 'inherit' });
	if (r.status !== 0) process.exit(r.status ?? 1);

	// install tsx inside sandbox
	console.log('📦 Installing tsx in sandbox…');
	r = spawnSync('npm', ['install', '--no-package-lock'], { cwd: sandboxDir, stdio: 'inherit' });
	if (r.status !== 0) process.exit(r.status ?? 1);

	// symlink client into sandbox
	const nmDir = path.join(sandboxDir, 'node_modules');
	fs.mkdirSync(nmDir, { recursive: true });
	const linkDest = path.join(nmDir, 'sml123');
	try { fs.symlinkSync(tsClientDir, linkDest, 'junction'); } catch {}

	// choose & run postinstall 
	const env = { ...process.env, ...(buildLocal ? { LOCAL_BUILD: 'TRUE' } : {}) };

	// call the compiled JS in dist/
	const postinstallJs = path.join(tsClientDir, 'dist', 'bin', 'postinstall.js');
	console.log('🏗 Running postinstall (compiled, LOCAL_BUILD unset)');
	r = spawnSync('node', [postinstallJs], { cwd: sandboxDir, stdio: 'inherit', env });

	if (r.status !== 0) process.exit(r.status ?? 1);

	// locate test models
	const onnxDir = path.join(stashDir, 'onnx');
	const surmlDir = path.join(stashDir, 'surml');

	const surmlFile = fs.readdirSync(surmlDir).find(f => f.endsWith('.surml'));
	const onnxFile  = fs.readdirSync(onnxDir).find(f => f.endsWith('.onnx'));
	if (!surmlFile || !onnxFile) {
		console.error(`❌ Need both .surml and .onnx files in ${stashDir}`);
		process.exit(1);
	}
	const surmlPath = path.join(surmlDir, surmlFile);
	const onnxPath  = path.join(onnxDir, onnxFile);
	console.log(`✅ SurML: ${surmlPath}`);
	console.log(`✅ ONNX:  ${onnxPath}`);

	// copy our smokeRunner.ts into sandbox
	console.log('Copying smoke runner into sandbox…');
	fs.copyFileSync(
		join(__dirname, './smoke_test_code.ts'),
		join(sandboxDir, 'smokeRunner.ts')
	);

	// run smokeRunner.ts with the two model paths
	console.log('Running smokeRunner.ts via tsx…');
	r = spawnSync(
		'npx',
		['tsx', '--tsconfig', 'tsconfig.json', 'smokeRunner.ts', surmlPath, onnxPath],
		{ cwd: sandboxDir, stdio: 'inherit' }
	);
	process.exit(r.status!);
}

main().catch(err => { console.error(err); process.exit(1); });
