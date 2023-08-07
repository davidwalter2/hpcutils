#!/usr/bin/env python3

import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor
import os
import logging

parser = argparse.ArgumentParser()
parser.add_argument("-j","--jobs", type=int, default=32)
parser.add_argument("-r","--recursive", action='store_true')
parser.add_argument("-e", "--empty", action='store_true')
parser.add_argument("--das", action='store_true', help="Treat 'source' as a DAS path")
parser.add_argument("-s", "--server", type=str, default = "eoscms.cern.ch")
parser.add_argument("--destination-xrd", action='store_true', help="Copy from local path into xrd area (default is reverse)")
parser.add_argument("--maxFiles", type=int, default=None)
parser.add_argument("--dryRun", action='store_true', help="Print command but don't copy")
parser.add_argument("source", type=str)
parser.add_argument("dest", type=str)

args = parser.parse_args();

logging.basicConfig(level=logging.INFO)

def build_das_filelist(das_path):
    # Not sure why shell=True is needed but it seems to be
    file_list = subprocess.check_output([f'dasgoclient --query="file dataset={das_path} status=*"'], shell=True, encoding='UTF-8')
    if file_list:
        return file_list.split()
    raise ValueError(f"Failed to find files for DAS path {das_path}")

def build_xrd_filelist(path, server, recursive):
    cmds = ["xrdfs", server, "ls", "-l"]

    if args.recursive:
        cmds += ["-R"]
        
    cmds.append(path)
        
    logging.info(f"Command to find all files: {' '.join(cmds)}")

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
    return lsfilenames

def build_local_filelist(path, recursive):
    import glob

    if not recursive:
        return glob.glob(path)
    else:
        return glob.glob(path+"/**", recursive=True)

if not args.das:
    source_files = build_xrd_filelist(args.source, args.server, args.recursive) if not args.destination_xrd else \
        build_local_filelist(args.source, args.recursive)
else:
    logging.info("Copying from DAS so using global xrootd redirector")
    args.server = "cms-xrd-global.cern.ch"
    source_files = build_das_filelist(args.source)

if args.maxFiles and args.maxFiles < len(source_files):
    logging.info(f"Copying the first {args.maxFiles} of {len(source_files)} valid files")
    source_files = source_files[:args.maxFiles]

basedirs = args.source.rstrip("/").split("/")[:-1] 
basedir = "/".join(basedirs)

make_name = lambda f,b,d,das=args.das: f.replace(b, d) if not das else f"{d}/{b}/"

if args.destination_xrd:
    infiles = source_files
    outfiles = [f"root://{args.server}/{make_name(f, basedir, args.dest)}" for f in source_files]
else:
    infiles = [f"root://{args.server}/{f}" for f in source_files]
    outfiles = [make_name(f, basedir, args.dest) for f in source_files]

outdirs = set([os.path.dirname(outfile) for outfile in outfiles])

for outdir in outdirs:
    if not os.path.isdir(outdir):
        if args.dryRun:
            logging.info(f"Would create output directory {outdir}")
        else:
            logging.info(f"Creating output directory {outdir}")
            os.makedirs(outdir)

if args.dryRun:
    logging.info(f"Will run the following command on {args.jobs} threads (example for the first file):")
    logging.info(f"--> xrdcp {infiles[0]} {outfiles[0]}")
    exit(0)

def xrdcp(files):
    subprocess.run(["xrdcp", files[0], files[1]])

with ThreadPoolExecutor(max_workers=args.jobs) as executor:
    executor.map(xrdcp, zip(infiles,outfiles))
    

