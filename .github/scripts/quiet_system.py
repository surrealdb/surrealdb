#!/bin/env python3

from pathlib import Path
from contextlib import contextmanager
import argparse
import subprocess
import os
import sys

SWAP_SYS = Path("/proc/sys/vm/swappiness")
ASLR_SYS = Path("/proc/sys/kernel/randomize_va_space")
CPU_SET = "0-16"

@contextmanager
def swappiness(value):
    existingValue = SWAP_SYS.read_text()
    SWAP_SYS.write_text(value)
    try:
        yield
    finally:
        if os.getuid() != 0:
            return
        try:
            SWAP_SYS.write_text(existingValue)
        except Exception as e:
            print("Failed to reset swappiness!",e)

@contextmanager
def aslr():
    ASLR_SYS.write_text("0")
    try:
        yield
    finally:
        if os.getuid() != 0:
            return
        try:
            ASLR_SYS.write_text("2")
        except Exception as e:
            print("Failed to reset address space randomization!",e)


@contextmanager
def performance_scaling():
    sysFiles = list(Path("/sys/devices/system/cpu").glob("cpu*/cpufreq/scaling_governor"))
    existingValues = [p.read_text() for p in sysFiles]
    for p in sysFiles:
        p.write_text("performance")
    try:
        yield
    finally:
        if os.getuid() != 0:
            return
        try:
            for p,v in zip(sysFiles,existingValues):
                p.write_text(v)
        except Exception as e:
            print("Failed to reset CPU scheduler!",e)


def runCmd(uid,gid,user, cmd):
    child = os.fork()
    if child == 0:
        os.setgid(gid)
        os.setuid(uid)
        env = os.environ.copy()
        env['HOME'] = f"/home/{user}"
        proc = subprocess.run(cmd,env=env)
        sys.exit(proc.returncode)
    else:
        while True:
            pid,exit_code = os.wait()
            if pid == child:
                return exit_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Quiet system",
        description="Python script to run commands in a more quiet environment for more consistant benchmarks",
    )
    parser.add_argument('command',nargs=argparse.REMAINDER)
    args = parser.parse_args()
    cmd = args.command
    if len(cmd) == 0:
        cmd = ["bash"]


    if not 'SUDO_UID' in os.environ:
        print("Script must be run with sudo")
        raise SystemExit

    userUid = int(os.environ['SUDO_UID'])
    userGid = int(os.environ['SUDO_GID'])
    user = os.environ['SUDO_USER']

    exit_code = 0
    with swappiness("10"):
        with aslr():
            with performance_scaling():
                print("System quieted, running command")
                exit_code = runCmd(userUid,userGid,user,cmd)
    sys.exit(exit_code)
