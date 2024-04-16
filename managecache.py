#!/usr/bin/env python3

import os
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-m","--mode", type=str, choices=["drop","load"], default="drop")
parser.add_argument("target", type=str, nargs=1)

args = parser.parse_args();

if args.mode == "drop":
    advice = os.POSIX_FADV_DONTNEED
elif args.mode == "load":
    advice = os.POSIX_FADV_WILLNEED
else:
    raise

rootdir = args.target[0]

for root,dirs,files in os.walk(rootdir):
    for f in files:
        fpath = f"{root}/{f}"
        cursize = os.path.getsize(fpath)
        with open(fpath,"rb") as f:
            os.posix_fadvise(f.fileno(), 0, cursize, advice)
