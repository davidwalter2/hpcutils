#!/usr/bin/env python3

import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("-j","--jobs", type=int, default=32)
parser.add_argument("-r","--recursive", action='store_true')
parser.add_argument("source", type=str, nargs=1)
parser.add_argument("dest", type=str, nargs=1)

args = parser.parse_args();

cmds = ["xrdfs", "eoscms.cern.ch", "ls"]

if args.recursive:
    cmds += ["-R"]
    
cmds += args.source
    
print(cmds)

res = subprocess.run(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
res.check_returncode()

lsfiles = str(res.stdout, 'utf-8').splitlines()

basedir = args.source[0].split("/")[:-1]
basedir = "/".join(basedir)
#print(basedir)

infiles = [f"root://eoscms.cern.ch/{f}" for f in lsfiles]
outfiles = [f.replace(basedir, args.dest[0]) for f in lsfiles]

def xrdcp(files):
    subprocess.run(["xrdcp", files[0], files[1]])


with ThreadPoolExecutor(max_workers=args.jobs) as executor:
    executor.map(xrdcp, zip(infiles,outfiles))
    

