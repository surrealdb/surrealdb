import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';


/**
 * Creates a file cache path for the model (creating the file cache if not there).
 *
 * cacheFolder: the directory to use for caching; default ".surmlcache"
 * returns: the path to the cache created with a unique id to prevent collisions.
 */
export function createFileCachePath(cacheFolder = '.surmlcache'): string {
	if (!fs.existsSync(cacheFolder)) {
		fs.mkdirSync(cacheFolder, { recursive: true });
	}
	const uniqueId = uuidv4();
	const fileName = `${uniqueId}.surml`;
	return path.join(cacheFolder, fileName);
}
