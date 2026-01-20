#!/usr/bin/env node
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import * as fs from 'fs';
import { spawnSync } from 'child_process';


const __filename = fileURLToPath(import.meta.url);
const __dirname  = dirname(__filename);


async function main() {
    const version = 'latest';

    // define dirs
    const scriptDir   = __dirname;
    const tsClientDir = join(scriptDir, '..');
    const sandboxDir  = join(tsClientDir, 'sandbox');
    const repoRoot    = join(tsClientDir, '..', '..');
    const stashDir    = join(repoRoot, 'modules', 'core', 'model_stash', 'onnx');

    // 1) recreate sandbox
    if (fs.existsSync(sandboxDir)) fs.rmSync(sandboxDir, { recursive: true, force: true });
    fs.mkdirSync(sandboxDir, { recursive: true });

    // 2) write minimal package.json & tsconfig.json
    fs.writeFileSync(
        join(sandboxDir, 'package.json'),
        JSON.stringify({
            name: 'sandbox',
            version: '1.0.0',
            private: true,
            type: 'module',
            dependencies: { 'sml123': version },
            devDependencies: { tsx: '^4.19.4', typescript: '^5.8.3' }
        }, null, 2)
    );
    fs.writeFileSync(
        join(sandboxDir, 'tsconfig.json'),
        JSON.stringify({
            compilerOptions: {
                module: 'NodeNext',
                target: 'ES2022',
                strict: true,
                esModuleInterop: true,
                resolveJsonModule: true
            }
        }, null, 2)
    );

    // 3) npm install (fires postinstall)
    console.log(`Installing sml123@${version}...`);
    let r = spawnSync('npm', ['install'], { cwd: sandboxDir, stdio: 'inherit' });
    if (r.status !== 0) process.exit(r.status!);

    // locate test models
    const onnxDir = join(stashDir, 'onnx');
    const surmlDir = join(stashDir, 'surml');

    const surmlFile = fs.readdirSync(surmlDir).find(f => f.endsWith('.surml'));
    const onnxFile  = fs.readdirSync(onnxDir).find(f => f.endsWith('.onnx'));
    if (!surmlFile || !onnxFile) {
        console.error(`❌ Need both .surml and .onnx files in ${stashDir}`);
        process.exit(1);
    }
    const surmlPath = join(surmlDir, surmlFile);
    const onnxPath  = join(onnxDir, onnxFile);
    console.log(`✅ SurML: ${surmlPath}`);
    console.log(`✅ ONNX:  ${onnxPath}`);

    // 5) copy our smokeRunner.ts into sandbox
    console.log('Copying smoke runner into sandbox…');
    fs.copyFileSync(
        join(__dirname, './smoke_test_code.ts'),
        join(sandboxDir, 'smokeRunner.ts')
    );

    // 6) run smokeRunner.ts with the two model paths
    console.log('Running smokeRunner.ts via tsx…');
    r = spawnSync(
        'npx',
        ['tsx', '--tsconfig', 'tsconfig.json', 'smokeRunner.ts', surmlPath, onnxPath],
        { cwd: sandboxDir, stdio: 'inherit' }
    );
    process.exit(r.status!);
}

main().catch(e => { console.error(e); process.exit(1); });
