#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
// @ts-ignore
import { SurMlFile, Engine } from 'sml123';

function run() {
	const [,, surmlPath, onnxPath] = process.argv;
	console.log(' Arguments:', { surmlPath, onnxPath });
	if (!surmlPath || !onnxPath) {
		console.error('Usage: smokeRunner.ts <model.surml> <raw.onnx>');
		process.exit(1);
	}

	console.log('🔧 Testing Engine enum');
	try {
		console.log('Engine enum:', Engine);
	} catch (e) {
		console.error('❌ Engine enum failed:', e);
	}

	let loadedFile: SurMlFile | undefined;
	console.log('🚀 Loading existing surml');
	try {
		loadedFile = SurMlFile.load(surmlPath, Engine.ONNX);
		console.log(' Initial metadata - ', { name: loadedFile.name, description: loadedFile.description, version: loadedFile.version });
		console.log(' rawCompute([1,2]) →', loadedFile.rawCompute([1,2]));
		console.log(' toBytes length →', loadedFile.toBytes().length, 'bytes');
	} catch (e) {
		console.error('❌ Load surml failed:', e);
	}

	let wrappedFile: SurMlFile | undefined;
	console.log('🚀 Wrapping raw onnx');
	try {
		const buf = fs.readFileSync(onnxPath);
		wrappedFile = new SurMlFile(buf, 'smokeTest', Engine.ONNX);
		console.log(' rawCompute([4,5]) →', wrappedFile.rawCompute([4,5]));
	} catch (e) {
		console.error('❌ Wrap onnx failed:', e);
	}

	if (wrappedFile) {
		console.log('⚙️ Exercising metadata methods');
		try {
			wrappedFile.addVersion('v1.2.3');
			wrappedFile.addName('smoke_test');
			wrappedFile.addDescription('smoke test');
			wrappedFile.addAuthor('Test Author');

			wrappedFile.addColumn('squarefoot');
			wrappedFile.addColumn('num_floors');
			wrappedFile.addNormaliser('squarefoot', 'z_score', 0.0, 1.0);
			wrappedFile.addNormaliser('num_floors', 'z_score', 0.0, 1.0);
			wrappedFile.addOutput('house_price', 'z_score', 0.0, 1.0);
			console.log(' New metadata - ', { 
				name: loadedFile.name, 
				description: loadedFile.description, 
				version: loadedFile.version,
				author: loadedFile.author,
			});

		} catch (e) {
			console.error('❌ Metadata methods failed:', e);
		}

		console.log(' bufferedCompute({"squarefoot": 5, "num_floors": 6}) →', (() => {
			try { return wrappedFile!.bufferedCompute({"squarefoot": 5, "num_floors": 6}); } catch (e) { console.error(' bufferedCompute failed:', e); return []; }
		})());

		console.log(' toBytes →', (() => {
			try {
				const b = wrappedFile!.toBytes();
				return b.length + ' bytes';
			} catch (e) {
				console.error(' toBytes failed:', e);
				return 'error';
			}
		})());

		console.log(' Save reload compute again');
		const tmp = path.join(process.cwd(), 'tmp.smoke.surml');
		try {
			wrappedFile.save(tmp);
			console.log(' Saved to', tmp);
		} catch (e) {
			console.error(' save failed:', e);
		}

		try {
			console.log(' Reloading');
			const reloaded = SurMlFile.load(tmp, Engine.ONNX);
			console.log(' rawCompute([7,8]) →', reloaded.rawCompute([7,8]));
		} catch (e) {
			console.error(' reload rawCompute failed:', e);
		}

		try { fs.unlinkSync(tmp); } catch {}
	}

	console.log('✅ Smoke runner complete');
}

try {
	run();
} catch (err) {
	console.error('❌ Uncaught error:', err);
	process.exit(1);
}
