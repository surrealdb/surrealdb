#!/usr/bin/env python3
"""Join SurrealDB and PostgreSQL-reference results per test and classify them.

Buckets per test id:
  pass_both          passes on both
  surrealdb_only     fails (or never ran after a failed hook) on SurrealDB, passes on the reference: a real gap
  fail_both          fails on both: environment or version noise
  reference_only     fails only on the reference
  not_run_surrealdb  passes on the reference but was never executed on SurrealDB
  skipped            skipped on either side
Failure causes are grouped by SQLSTATE when the error carried one, otherwise by the message with
numbers, quoted names and ids stripped. Reads results/upstream/<orm>-<suite>-<target>.json, writes
classification.json (per test) and summary.json / summary.md (counts and top causes).
"""
import collections
import json
import re

from common import RESULTS, log

TOP = 12


def normalise(message):
    m = (message or '').strip().splitlines()[0] if (message or '').strip() else '(no message)'
    m = re.sub(r'"[^"]*"|\'[^\']*\'|`[^`]*`', '"…"', m)
    m = re.sub(r'\b[0-9a-f]{8}-[0-9a-f-]{27}\b', '<uuid>', m)
    m = re.sub(r'\d+', 'N', m)
    return m[:140]


def cause(t):
    base = normalise(t.get('message'))
    return f"{t['code']}: {base}" if t.get('code') else base


def load(orm, suite, target):
    p = RESULTS / f'{orm}-{suite}-{target}.json'
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding='utf-8'))


def classify_suite(orm, suite):
    s, r = load(orm, suite, 'surrealdb'), load(orm, suite, 'reference')
    if not s or not r:
        return None
    st = {t['id']: t for t in s['tests']}
    rt = {t['id']: t for t in r['tests']}
    hook_by_file = {}
    for t in s['tests']:
        if t['status'] == 'fail' and t['id'].endswith('[hook]'):
            hook_by_file.setdefault(t['id'].split('::', 1)[0], t)
    rows = []
    for i in sorted(set(st) | set(rt)):
        a, b = st.get(i), rt.get(i)
        sa, sb = a['status'] if a else 'missing', b['status'] if b else 'missing'
        if 'skip' in (sa, sb):
            bucket = 'skipped'
        elif sa == 'pass' and sb == 'pass':
            bucket = 'pass_both'
        elif sa == 'fail' and sb == 'pass':
            bucket = 'surrealdb_only'
        elif sa == 'fail' and sb == 'fail':
            bucket = 'fail_both'
        elif sa == 'fail' and sb == 'missing':
            bucket = 'surrealdb_only'
        elif sa == 'pass' and sb == 'fail':
            bucket = 'reference_only'
        elif sa == 'missing' and sb == 'fail':
            bucket = 'reference_only'
        elif sa == 'missing' and sb == 'pass':
            bucket = 'not_run_surrealdb'
        elif sa == 'pass' and sb == 'missing':
            bucket = 'pass_both'
        else:
            bucket = 'skipped'
        blocker = hook_by_file.get(i.split('::', 1)[0]) if bucket == 'not_run_surrealdb' else None
        rows.append(dict(id=i, bucket=bucket, surrealdb=sa, reference=sb,
                         surrealdb_cause=cause(a) if sa == 'fail' else (cause(blocker) if blocker else None),
                         surrealdb_query=(a or blocker or {}).get('query'),
                         surrealdb_sqlstate=(a or {}).get('code') if sa == 'fail' else None,
                         reference_cause=cause(b) if sb == 'fail' else None))
    counts = collections.Counter(x['bucket'] for x in rows)
    counts['skipped'] += max(s.get('skipped_both', 0), r.get('skipped_both', 0))
    causes = {}
    for bucket in ('surrealdb_only', 'fail_both'):
        sel = [x for x in rows if x['bucket'] == bucket and x['surrealdb_cause']]
        c = collections.Counter(x['surrealdb_cause'] for x in sel)
        example = {}
        for x in sel:
            example.setdefault(x['surrealdb_cause'], x)
        causes[bucket] = [dict(cause=k, count=v, example_test=example[k]['id'], example_query=example[k].get('surrealdb_query'))
                          for k, v in c.most_common(TOP)]
    blocked = [x for x in rows if x['bucket'] == 'not_run_surrealdb' and x['surrealdb_cause']]
    c = collections.Counter(x['surrealdb_cause'] for x in blocked)
    causes['not_run_surrealdb'] = [dict(cause=k, count=v) for k, v in c.most_common(TOP)]
    raw = lambda doc: dict(collections.Counter(t['status'] for t in doc['tests']), capped=doc.get('capped', False), wall_s=doc.get('wall_s'), exit_code=doc.get('exit_code'), guard_blocked=doc.get('guard_blocked', 0))
    guard = max(doc.get('guard_blocked', 0) for doc in (s, r))
    not_measured = guard > 0 and guard * 2 > len(rows) - sum(1 for x in rows if x['bucket'] == 'skipped')
    reference_pass = counts['pass_both'] + counts['surrealdb_only'] + counts['not_run_surrealdb']
    summary = dict(orm=orm, suite=suite, version=s.get('version'), source_commit=s.get('commit'),
                   raw=dict(surrealdb=raw(s), reference=raw(r)), buckets=dict(counts),
                   surrealdb_pass_rate_of_reference_passing=None if not_measured or not reference_pass else round(counts['pass_both'] / reference_pass, 4),
                   not_measured=not_measured,
                   top_causes=causes)
    return summary, rows


def markdown(summaries):
    out = ['# Upstream ORM suites against the SurrealDB PostgreSQL listener', '',
           'Control: PostgreSQL 18.4 (embedded). "SurrealDB pass rate" is tests passing on both over tests passing on the reference.', '']
    for s in summaries:
        b = s['buckets']
        out += [f"## {s['orm']} / {s['suite']} ({s['version']})", '',
                '| | pass | fail | skip |', '|---|---|---|---|']
        for t in ('surrealdb', 'reference'):
            r = s['raw'][t]
            out.append(f"| {t} | {r.get('pass', 0)} | {r.get('fail', 0)} | {r.get('skip', 0)} |")
        blocked = max(s['raw']['surrealdb'].get('guard_blocked', 0), s['raw']['reference'].get('guard_blocked', 0))
        if blocked:
            out += ['', f'{blocked} test(s) or shard(s) were stopped by the ORM\'s own AI-agent safety guard before touching the database and are excluded; export the guard variable yourself to run them.']
        rate = s['surrealdb_pass_rate_of_reference_passing']
        out += ['', '| classification | tests |', '|---|---|']
        for k in ('pass_both', 'surrealdb_only', 'not_run_surrealdb', 'fail_both', 'reference_only', 'skipped'):
            out.append(f'| {k} | {b.get(k, 0)} |')
        if s['not_measured']:
            out += ['', 'NOT MEASURED: the safety guard blocked most of this suite, so the buckets above do not compare like with like.', '']
        else:
            out += ['', f"SurrealDB pass rate: {rate:.1%}" if rate is not None else 'SurrealDB pass rate: n/a', '']
        for bucket, title in (('surrealdb_only', 'Top causes, fails only on SurrealDB'), ('not_run_surrealdb', 'Never ran on SurrealDB because a setup hook failed, by cause'), ('fail_both', 'Top causes, fails on both')):
            if s['top_causes'][bucket]:
                out += [f'**{title}**', '', '| count | cause |', '|---|---|']
                out += [f"| {c['count']} | {c['cause'].replace('|', '/')} |" for c in s['top_causes'][bucket]]
                out.append('')
    return '\n'.join(out) + '\n'


def compact(everything):
    """Drop tests skipped on both sides from the stored per-target files, keeping a count, so the
    committed results stay small. Classification reads the count back."""
    for key, rows in everything.items():
        orm, suite = key.split('/')
        both = {x['id'] for x in rows if x['bucket'] == 'skipped' and x['surrealdb'] in ('skip', 'missing') and x['reference'] in ('skip', 'missing')}
        for target in ('surrealdb', 'reference'):
            p = RESULTS / f'{orm}-{suite}-{target}.json'
            doc = json.loads(p.read_text(encoding='utf-8'))
            kept = [t for t in doc['tests'] if t['id'] not in both]
            doc['skipped_both'] = doc.get('skipped_both', 0) + len(doc['tests']) - len(kept)
            doc['tests'] = kept
            p.write_text(json.dumps(doc, separators=(',', ':')) + '\n')


def main():
    summaries, everything = [], {}
    suites = set()
    for p in RESULTS.glob('*-*.json'):
        doc = json.loads(p.read_text(encoding='utf-8'))
        if isinstance(doc, dict) and 'target' in doc:
            suites.add((doc['orm'], doc['suite']))
    suites = sorted(suites)
    for orm, suite in suites:
        res = classify_suite(orm, suite)
        if res:
            summaries.append(res[0])
            everything[f'{orm}/{suite}'] = res[1]
    slim = {k: [x for x in v if x['bucket'] not in ('pass_both', 'skipped')] for k, v in everything.items()}
    (RESULTS / 'classification.json').write_text(json.dumps(slim, separators=(',', ':')) + '\n')
    (RESULTS / 'summary.json').write_text(json.dumps(summaries, indent=2) + '\n')
    (RESULTS / 'summary.md').write_text(markdown(summaries))
    if '--compact' in __import__('sys').argv:
        compact(everything)
    log('wrote', RESULTS / 'summary.md')
    print(markdown(summaries))


if __name__ == '__main__':
    main()
