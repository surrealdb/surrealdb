// Node exits with status 0 once its event loop is empty, even when a promise never settles, so
// a smoke test that deadlocks on the JavaScript executor would pass. The exit handler fails any
// exit that `run_smoke` did not reach.
const t0 = Date.now();
let settled = false;
process.on('exit', (code) => {
  if (!settled && code === 0) {
    console.error('node: the event loop emptied before run_smoke() settled');
    process.exitCode = 1;
  }
});
require('./pkg/diskann_web_guest.js').run_smoke().then(
  (log) => { settled = true; console.log(log); console.log(`node: finished in ${((Date.now() - t0) / 1000).toFixed(1)} s`); },
  (err) => { settled = true; console.error(String(err)); process.exitCode = 1; },
);
