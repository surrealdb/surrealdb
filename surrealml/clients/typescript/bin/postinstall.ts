import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawnSync } from 'child_process';
import * as https from 'https';
import * as tar from 'tar';
import * as unzipper from 'unzipper';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);


/** ===================================== Define the paths for the install ================================================= **/

function getCLibName(): string {
	const system = os.type(); // e.g. 'Linux', 'Darwin', 'Windows_NT'
	if (system === 'Linux') {
		return 'libc_wrapper.so';
	} else if (system === 'Darwin') {
		return 'libc_wrapper.dylib';
	} else if (system.startsWith('Windows')) {
		return 'c_wrapper.dll';
	}
	throw new Error(`Unsupported system: ${system}`);
}

const BIN_PATH = __dirname;
const DIR_PATH = path.join(BIN_PATH, '..');
const ROOT_PATH = path.join(DIR_PATH, '..', '..', '..');
const C_PATH = path.join(ROOT_PATH, 'modules', 'c-wrapper');
const BINARY_PATH = path.join(ROOT_PATH, 'target', 'release', getCLibName());
const ROOT_BINARY_PATH = path.join(ROOT_PATH, 'target', 'release', getCLibName());

const OVERRIDE_OS = process.env.TARGET_OS;
const OVERRIDE_ARCH = process.env.TARGET_ARCH;

let DYNAMIC_LIB_VERSION: string;
try {
	const cfgPath = path.join(BIN_PATH, '..', '..', 'config.json');
	const raw = fs.readFileSync(cfgPath, 'utf-8');
	const cfg = JSON.parse(raw);
	if (typeof cfg.dynamic_lib_version !== 'string') {
		throw new Error('dynamic_lib_version is missing or not a string');
	};
  	DYNAMIC_LIB_VERSION = cfg.dynamic_lib_version;

} catch (err: any) {
	console.error(`❌ Cannot load dynamic_lib_version: ${err.message}`);
	process.exit(1);
}

const OS_NAME = OVERRIDE_OS || process.platform; // 'linux', 'darwin', 'win32', etc.
let ARCH = (OVERRIDE_ARCH || os.arch()).toLowerCase(); // 'x64', 'arm64', etc.
if (ARCH === 'x64') ARCH = 'x86_64'; // normalize

const raw = os.type().split('_')[0];
if (!raw) {
	throw new Error(`Could not detect operating system name (os.type() returned '${os.type()}')`);
}
const SYSTEM = raw.toLowerCase(); // 'linux', 'darwin', 'windows'

const ROOT_DEP_DIR = path.join(os.homedir(), 'surrealml_deps');
fs.mkdirSync(ROOT_DEP_DIR, { recursive: true });

const DYNAMIC_LIB_DIR = path.join(ROOT_DEP_DIR, 'core_ml_lib', DYNAMIC_LIB_VERSION);
const DYNAMIC_LIB_DIST = path.join(DYNAMIC_LIB_DIR, getCLibName());
const DYNAMIC_LIB_DOWNLOAD_CACHE = path.join(DYNAMIC_LIB_DIR, 'download_cache.tgz');
fs.mkdirSync(DYNAMIC_LIB_DIR, { recursive: true });

function getLibUrl(): [string, string] {
	if (OS_NAME.startsWith('linux')) {
		if (ARCH === 'x86_64') {
			const name = `surrealml-v${DYNAMIC_LIB_VERSION}-x86_64-unknown-linux-gnu.tar.gz`;
			return [
				`https://github.com/surrealdb/surrealml/releases/download/v${DYNAMIC_LIB_VERSION}/${name}`,
				name
			];
		} else if (ARCH === 'arm64' || ARCH === 'aarch64') {
			const name = `surrealml-v${DYNAMIC_LIB_VERSION}-arm64-unknown-linux-gnu.tar.gz`;
			return [
				`https://github.com/surrealdb/surrealml/releases/download/v${DYNAMIC_LIB_VERSION}/${name}`,
				name
			];
		}
	} else if (OS_NAME === 'darwin') {
		if (ARCH === 'x86_64') {
			const name = `surrealml-v${DYNAMIC_LIB_VERSION}-x86_64-apple-darwin.tar.gz`;
			return [
				`https://github.com/surrealdb/surrealml/releases/download/v${DYNAMIC_LIB_VERSION}/${name}`,
				name
			];
		} else if (ARCH === 'arm64') {
			const name = `surrealml-v${DYNAMIC_LIB_VERSION}-arm64-apple-darwin.tar.gz`;
			return [
				`https://github.com/surrealdb/surrealml/releases/download/v${DYNAMIC_LIB_VERSION}/${name}`,
				name
			];
		}
	} else if (OS_NAME === 'win32') {
		if (ARCH === 'x86_64') {
			const name = `surrealml-v${DYNAMIC_LIB_VERSION}-x86_64-pc-windows-msvc.tar.gz`;
			return [
				`https://github.com/surrealdb/surrealml/releases/download/v${DYNAMIC_LIB_VERSION}/${name}`,
				name
			];
		} else if (ARCH === 'arm64') {
			// no arm64 Windows build currently
		}
	}
	throw new Error(`Unsupported platform or architecture: ${OS_NAME}`);
}

async function downloadAndExtractCLib(): Promise<string> {
    const [libUrl, extractedDir] = getLibUrl();
  
    if (!fs.existsSync(DYNAMIC_LIB_DOWNLOAD_CACHE)) {
      console.log(`Downloading surrealML lib from ${libUrl}`);
      await new Promise<void>((resolve, reject) => {
        const doRequest = (url: string) => {
          https.get(url, res => {
            // Follow redirect once
            if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
              return doRequest(res.headers.location);
            }
            // Only accept “200 OK”
            if (res.statusCode !== 200) {
              return reject(new Error(`Failed to download ${url}: status ${res.statusCode}`));
            }
            const file = fs.createWriteStream(DYNAMIC_LIB_DOWNLOAD_CACHE);
            res.pipe(file);
            file.on('finish', () => {
                file.close(err => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
            file.on('error', err => {
                reject(err);
            });
          }).on('error', reject);
        };
        doRequest(libUrl);
      });
    } else {
      console.log(`the ${DYNAMIC_LIB_DOWNLOAD_CACHE} already exists so not downloading surrealML lib`);
    }
  
    // extraction
    if (DYNAMIC_LIB_DOWNLOAD_CACHE.endsWith('.tgz')) {
      await tar.x({
        file: DYNAMIC_LIB_DOWNLOAD_CACHE,
        cwd: DYNAMIC_LIB_DIR
      });
    } else if (DYNAMIC_LIB_DOWNLOAD_CACHE.endsWith('.zip')) {
      await fs.createReadStream(DYNAMIC_LIB_DOWNLOAD_CACHE)
        .pipe(unzipper.Extract({ path: DYNAMIC_LIB_DIR }))
        .promise();
    }
  
    return extractedDir;
}

/** ===================================== Build / Download Logic ================================================= **/

(async () => {
	let BUILD_FLAG = false;

	if (!fs.existsSync(DYNAMIC_LIB_DIST) && process.env.LOCAL_BUILD === 'TRUE') {
		console.log('building core ML lib locally');
		spawnSync('cargo build --release', { cwd: C_PATH, shell: true, stdio: 'inherit' });
		ARCH = (OVERRIDE_ARCH || os.arch()).toLowerCase();
		if (ARCH === 'x64') ARCH = 'x86_64';

		if (fs.existsSync(BINARY_PATH)) {
			fs.copyFileSync(BINARY_PATH, DYNAMIC_LIB_DIST);
		} else if (fs.existsSync(ROOT_BINARY_PATH)) {
			fs.copyFileSync(ROOT_BINARY_PATH, DYNAMIC_LIB_DIST);
		}

		BUILD_FLAG = true;

	} else {
		if (!fs.existsSync(DYNAMIC_LIB_DIST)) {
			console.log('downloading the core ML lib');
			const libPath = await downloadAndExtractCLib();
			fs.unlinkSync(DYNAMIC_LIB_DOWNLOAD_CACHE);

			// build path to the freshly-extracted library
			const downloadedFile = path.join(DYNAMIC_LIB_DIR, getCLibName());
			if (!fs.existsSync(downloadedFile)) {
				throw new Error(`Expected shared lib at ${downloadedFile}, but none was found`);
			}

            // copy it into the user's deps cache
            fs.mkdirSync(path.dirname(DYNAMIC_LIB_DIST), { recursive: true });
            fs.copyFileSync(downloadedFile, DYNAMIC_LIB_DIST);
		}
	}
})().catch(err => {
	console.error(err);
	process.exit(1);
});
