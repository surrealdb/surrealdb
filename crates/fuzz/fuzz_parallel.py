#!/usr/bin/env python3

# Simple script to run fuzzing with N cpu's terminating on first error.

import multiprocessing
import subprocess
import asyncio
import sys

CPU_COUNT = multiprocessing.cpu_count()

async def run(cmd, pipe=True):
    if pipe:
        proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
    else:
        proc = await asyncio.create_subprocess_shell(cmd)
    return proc

async def wait_on_proc(proc):
    await proc.wait()
    return proc

async def main():
    b_proc = await run("cargo fuzz build --fuzz-dir ./ fuzz_format", False)
    await b_proc.wait();
    if b_proc.returncode != 0:
        print("Failed to build")
        return

    procs = [await run("cargo fuzz run --fuzz-dir ./ fuzz_format") for i in range(CPU_COUNT)]
    tasks = [asyncio.create_task(wait_on_proc(procs[i])) for i in range(CPU_COUNT)]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    res = done.pop().result()
    sys.stdout.buffer.write(await res.stdout.read())
    sys.stdout.buffer.write(await res.stderr.read())

    for t in tasks:
        t.cancel()

    for p in procs:
        try:
            p.kill()
            await p.wait()
        except:
            pass


asyncio.run(main())
