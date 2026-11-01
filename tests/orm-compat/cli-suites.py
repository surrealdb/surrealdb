import os, json, subprocess, pathlib, time
root = pathlib.Path(__file__).resolve().parent
(root / 'cli-logs').mkdir(exist_ok=True)
results = []
# Bounds each schema-tool command; drizzle-kit's introspection of the debug build takes about a minute.
CLI_TIMEOUT_S = 300

def run(target, orm, name, args, url, extra=None, check=None, timeout=CLI_TIMEOUT_S):
    env = dict(os.environ, DATABASE_URL=url, PRISMA_HIDE_UPDATE_MESSAGE='1')
    env['NODE_OPTIONS'] = '--require ' + str(root / 'trace-pg.cjs')
    env['ORM_TRACE_PATH'] = str(root / 'cli-logs' / f'{target}-{orm}-{name}-sql.json')
    if extra:
        env.update(extra)
    started = time.monotonic()
    try:
        r = subprocess.run(args, cwd=root, env=env, capture_output=True, text=True, timeout=timeout)
        log = r.stdout + '\n' + r.stderr
        ok = r.returncode == 0
        if ok and check:
            ok = check(r.stdout)
        if ok and name.startswith('migration-'):
            probe = 'import pg from \'pg\';const c=new pg.Client({connectionString:process.argv[1]});await c.connect();await c.query(\'SELECT id,name,age FROM "CompatUser" LIMIT 1\');await c.query(\'SELECT id,title FROM "CompatPost" LIMIT 1\');await c.end();'
            verified = subprocess.run(['node', '--input-type=module', '-e', probe, url], cwd=root, env=env, capture_output=True, text=True, timeout=10)
            ok = verified.returncode == 0
            if not ok:
                log += '\nSchema verification failed:\n' + verified.stderr
        record = {'target': target, 'orm': orm, 'name': name, 'status': 'pass' if ok else 'fail', 'exit_code': r.returncode, 'command': args, 'log': f'cli-logs/{target}-{orm}-{name}.log'}
    except subprocess.TimeoutExpired as e:
        out = lambda v: v.decode(errors='replace') if isinstance(v, bytes) else (v or '')
        log = str(e) + '\n' + out(e.stdout) + '\n' + out(e.stderr)
        record = {'target': target, 'orm': orm, 'name': name, 'status': 'fail', 'error': 'timeout', 'command': args, 'log': f'cli-logs/{target}-{orm}-{name}.log'}
    record['seconds'] = round(time.monotonic() - started, 1)
    (root / record['log']).write_text(log)
    results.append(record)
    print(target, orm, name, record['status'], f"{record['seconds']}s", flush=True)
    return record['status'] == 'pass'
for target in os.environ.get('ORM_COMPAT_TARGETS', 'reference,surrealdb').split(','):
    ref = target == 'reference'
    port = 15433 if ref else 15432
    base = f'postgresql://root:root@127.0.0.1:{port}/' if ref else f'postgresql://root@127.0.0.1:{port}/'

    def url(db):
        return base + db + ('' if ref else '?options=-c%20dialect%3Dpgsql')

    def prisma_url(db):
        # Prisma's schema engine keeps only the first path segment of the database, so the
        # namespace is given as a startup option and the path names the database alone.
        return base + db + ('' if ref else '?options=-c%20namespace%3Dorm%20-c%20dialect%3Dpgsql')
    if ref:
        code = "import pg from 'pg';const c=new pg.Client({host:'127.0.0.1',port:15433,user:'root',password:'root',database:'postgres'});await c.connect();for(const n of ['ref_prisma_migrate','ref_drizzle_migrate']){await c.query('DROP DATABASE IF EXISTS '+n);await c.query('CREATE DATABASE '+n)}await c.end();"
        subprocess.run(['node', '--input-type=module', '-e', code], check=True)
    introspect = prisma_url('postgres' if ref else 'prisma')
    run(target, 'prisma', 'introspection', ['node', 'node_modules/prisma/build/index.js', 'db', 'pull', '--print'], introspect, check=lambda text: 'model CompatUser' in text and 'model CompatPost' in text)
    migrate = prisma_url('ref_prisma_migrate' if ref else 'prisma_migrate')
    if run(target, 'prisma', 'migration-apply', ['node', 'node_modules/prisma/build/index.js', 'migrate', 'deploy'], migrate):
        run(target, 'prisma', 'migration-repeat', ['node', 'node_modules/prisma/build/index.js', 'migrate', 'deploy'], migrate)
    else:
        results.append({'target': target, 'orm': 'prisma', 'name': 'migration-repeat', 'status': 'blocked', 'reason': 'migration-apply failed'})
    out = f'drizzle-introspect-{target}'
    run(target, 'drizzle', 'introspection', ['node', 'node_modules/drizzle-kit/bin.cjs', 'pull'], url('postgres' if ref else 'orm/drizzle'), extra={'DRIZZLE_OUT': out}, check=lambda _: (root / out / 'schema.ts').exists() and 'CompatUser' in (root / out / 'schema.ts').read_text())
    migrate = url('ref_drizzle_migrate' if ref else 'orm/drizzle_migrate')
    if run(target, 'drizzle', 'migration-apply', ['node', 'node_modules/drizzle-kit/bin.cjs', 'migrate'], migrate):
        run(target, 'drizzle', 'migration-repeat', ['node', 'node_modules/drizzle-kit/bin.cjs', 'migrate'], migrate)
    else:
        results.append({'target': target, 'orm': 'drizzle', 'name': 'migration-repeat', 'status': 'blocked', 'reason': 'migration-apply failed'})
(root / 'cli-results.json').write_text(json.dumps(results, indent=2))
raise SystemExit(1 if any((r['status'] != 'pass' for r in results)) else 0)
