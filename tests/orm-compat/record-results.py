"""Record paired compatibility results after both backends have completed their suites."""
import collections
import json
import os
import pathlib
import subprocess

root = pathlib.Path(__file__).resolve().parent
os.chdir(root)

def read(name):
    return json.loads((root / name).read_text())

def clean(value):
    return json.loads(json.dumps(value).replace(str(root), '<harness>').replace('/private/tmp/surreal-orm-compat', '<harness>'))

runtime = read('results.json')
reference = read('reference-results.json')
cli = read('cli-results.json')
for result in cli:
    if 'log' in result:
        path = root / result['log']
        result['output'] = path.read_text()
        trace = path.with_name(path.stem + '-sql.json')
        if trace.exists():
            result['driver_errors'] = [row for row in json.loads(trace.read_text()) if 'error' in row]

reference_cases = reference['results'] + [r for r in cli if r['target'] == 'reference']
target_cases = runtime['results'] + [r for r in cli if r['target'] == 'surrealdb']
assert all(r['status'] == 'pass' for r in reference_cases), 'Reference failures invalidate the comparison'
identity = lambda rows: {(r['orm'], r['name']) for r in rows}
assert identity(reference_cases) == identity(target_cases), 'The backends must cover the same scenarios'
assert len(identity(target_cases)) == len(target_cases), 'Scenario identities must be unique'

source = os.environ.get('SOURCE_COMMIT') or subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
tree = os.environ.get('SOURCE_TREE') or subprocess.check_output(['git', 'rev-parse', source + '^{tree}'], text=True).strip()
probe = """
import pg from 'pg';
const c = new pg.Client({host:'127.0.0.1',port:15433,user:'root',password:'root',database:'postgres'});
await c.connect(); const result = await c.query('select version()'); await c.end();
console.log(JSON.stringify({measuredAt:new Date().toISOString(),node:process.version,platform:process.platform,
architecture:process.arch,reference:result.rows[0].version}));
"""
environment = json.loads(subprocess.check_output(['node', '--input-type=module', '-e', probe], text=True))
environment.update(sourceCommit=source, sourceTree=tree, versions=read('package.json')['dependencies'])
summary = {'environment': environment, 'scope': 'Targeted compatibility probes; not upstream ORM suites or a performance benchmark', 'reference': {}, 'surrealdb': {}}
for target, rows in [('reference', reference_cases), ('surrealdb', target_cases)]:
    for orm in ['prisma', 'drizzle', 'typeorm']:
        counts = collections.Counter(r['status'] for r in rows if r['orm'] == orm)
        summary[target][orm] = {status: counts[status] for status in ['pass', 'fail', 'blocked']}

out = root / 'results'
out.mkdir(exist_ok=True)
for name, value in [('surrealdb.json', runtime), ('postgresql.json', reference), ('schema-tools.json', cli), ('summary.json', summary)]:
    (out / name).write_text(json.dumps(clean(value), indent=2) + '\n')
print(json.dumps(summary['surrealdb'], indent=2))
