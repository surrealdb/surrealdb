#!/usr/bin/env python3
"""Run upstream ORM test suites against SurrealDB and the PostgreSQL reference, then classify.

usage: run.py [--targets surrealdb,reference] [--skip-classify] [ORM ...]
ORMs: typeorm, drizzle, drizzle-kit, prisma (default: all). The listeners must already be running;
run.sh starts them and restarts the SurrealDB listener between ORMs.
"""
import argparse
import importlib
import socket
import sys

from common import *

ADAPTERS = ['typeorm', 'drizzle', 'drizzle-kit', 'prisma']


def listener_alive(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=1):
            return True
    except OSError:
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--targets', default=','.join(TARGETS))
    ap.add_argument('--skip-classify', action='store_true')
    ap.add_argument('orms', nargs='*')
    args = ap.parse_args()
    orms = args.orms or ADAPTERS
    targets = args.targets.split(',')
    for orm in orms:
        if orm not in ADAPTERS:
            sys.exit(f'unknown ORM {orm}; choose from {ADAPTERS}')
    for port, name in ((15432, 'SurrealDB listener'), (15433, 'PostgreSQL reference')):
        if not listener_alive(port):
            sys.exit(f'{name} is not listening on {port}')
    for orm in orms:
        mod = importlib.import_module(orm.replace('-', '_'))
        ctx = mod.setup()
        for target in targets:
            suites = mod.run_target(target, *ctx)
            if isinstance(suites, tuple):
                suites = [suites]
            for suite, meta, tests in suites:
                write_result(orm, suite, target, meta, tests)
    if not args.skip_classify:
        import classify
        classify.main()


if __name__ == '__main__':
    main()
