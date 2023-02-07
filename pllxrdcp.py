#!/usr/bin/env python3

import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("-j","--jobs", type=int, default=32)
parser.add_argument("-r","--recursive", action='store_true')
parser.add_argument("-e", "--empty", action='store_true')
parser.add_argument("-s", "--server", type=str, default = "eoscms.cern.ch")
parser.add_argument("--maxFiles", type=int, default=None)
parser.add_argument("--dryRun", action='store_true', help="Print command but don't copy")
parser.add_argument("source", type=str)
parser.add_argument("dest", type=str)

args = parser.parse_args();

cmds = ["xrdfs", args.server, "ls", "-l"]

if args.recursive:
    cmds += ["-R"]
    
cmds.append(args.source)
    
print("Command to find all files:", cmds)

res = subprocess.run(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
res.check_returncode()

lsfiles = str(res.stdout, 'utf-8').splitlines()

lsfilenames = []
for f in lsfiles:
    fsplit = f.split(" ")
    filesize = fsplit[-2]
    filename = fsplit[-1]
    if args.empty or filesize != "0":
        lsfilenames.append(filename)
if args.maxFiles and args.maxFiles < len(lsfilenames):
    print(f"INFO: copying the first {args.maxFiles} of {len(lsfilenames)} valid files")
    lsfilenames = lsfilenames[:args.maxFiles]

basedir = args.source.rstrip("/").split("/")[:-1]
basedir = "/".join(basedir)

infiles = [f"root://{args.server}/{f}" for f in lsfilenames]
outfiles = [f.replace(basedir, args.dest) for f in lsfilenames]

if args.dryRun:
    print(f"Will run the following command on {args.jobs} threads (example for the first file):")
    print(f"--> xrdcp {infiles[0]} {outfiles[0]}")
    exit(0)

def xrdcp(files):
    subprocess.run(["xrdcp", files[0], files[1]])


with ThreadPoolExecutor(max_workers=args.jobs) as executor:
    executor.map(xrdcp, zip(infiles,outfiles))
    

