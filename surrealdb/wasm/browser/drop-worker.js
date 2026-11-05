import init, { SurrealWasmEngine } from "../dist/index.js";
await init({ module_or_path: new URL("../dist/index_bg.wasm", import.meta.url).href });
const name = "drop-" + Math.random().toString(36).slice(2);
const r = {};
const a = await SurrealWasmEngine.connect("surrealkv://" + name);
a.free(); // no close(): the drop must close the datastore and release the lock
const t0 = performance.now();
try {
  const b = await SurrealWasmEngine.connect("surrealkv://" + name);
  r.reopened = true; r.ms = Math.round(performance.now() - t0);
  await b.close(); b.free();
} catch (e) { r.reopened = false; r.error = String(e); r.ms = Math.round(performance.now() - t0); }
// two concurrent closes: the second must not resolve before the first shutdown is done
const c = await SurrealWasmEngine.connect("surrealkv://" + name + "-two");
let firstDone = false;
const p1 = c.close().then(() => { firstDone = true; });
const p2 = c.close();
await p2;
r.secondCloseWaitedForFirst = firstDone;
await p1;
c.free();
// close while a request is still running: the request settles before close resolves, and free does not trap
const enc = new TextEncoder();
const t = (str) => { const b = enc.encode(str); return [0x60 + b.length, ...b]; };
const slow = new Uint8Array([0xA3, ...t("id"), 0x01, ...t("method"), ...t("query"), ...t("params"), 0x81, ...t("SLEEP 400ms")]);
const d = await SurrealWasmEngine.connect("mem://");
let requestDone = false;
const running = d.execute(slow).then(() => { requestDone = true; }, () => { requestDone = true; });
await new Promise((res) => setTimeout(res, 50));
await d.close();
r.requestSettledBeforeCloseResolved = requestDone;
try { d.free(); r.freeAfterCloseOk = true; } catch (e) { r.freeAfterCloseOk = String(e); }
await running;
postMessage(r);
