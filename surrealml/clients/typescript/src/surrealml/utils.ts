/**
 * Defines utility functions for SurrealML.
 */
import { readFileSync } from "fs";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";


/**
 * Load and return the dynamic_lib_version value from a config.json file
 * that lives one level above this module.
 *
 * Works both in development (when running directly from src/) and in
 * the published build (when running from dist/src/ after copying the JSON).
 *
 * Returns the string found under "dynamic_lib_version" in config.json.
 *
 * Throws an Error on any failure: if the file is missing, the JSON
 * is invalid, or the dynamic_lib_version key is absent or wrong type.
 */
export function readDynamicLibVersion(): string {
    const here = dirname(fileURLToPath(import.meta.url));
    // Go up past the dist level to the root of the repo (or package in production)
    const cfgPath = resolve(here, "..", "..", "..", "config.json");

    try {
        const raw = readFileSync(cfgPath, "utf-8");
        const cfg = JSON.parse(raw);
        if (typeof cfg.dynamic_lib_version !== "string") {
            throw new Error(`"dynamic_lib_version" is missing or not a string`);
        }

        return cfg.dynamic_lib_version;
        
    } catch (e: any) {
        throw new Error(`Error loading dynamic_lib_version from ${cfgPath}: ${e.message}`);
    }
}
