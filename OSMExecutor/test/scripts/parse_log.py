#!/usr/bin/python3

import os
import zipfile
import sys
import glob
import gzip
import time
from subprocess import call
from operator import itemgetter

task_name = sys.argv[1]

root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
logs_dir = os.path.join(root, "logs")
asterixdb = os.path.join(root, "asterixdb", "opt", "local")

is_level = len(glob.glob(os.path.join(asterixdb, "data", "red", "storage", "partition_0", "Level_Spatial", "Spatial_Table", "0",
                 "rtreeidx", "0_*"))) > 0

io_flags = (
    os.path.join(asterixdb, "data", "red", "storage", "partition_0", "Level_Spatial", "Spatial_Table", "0",
                 "Spatial_Table", "is_flushing"),
    os.path.join(asterixdb, "data", "red", "storage", "partition_0", "Level_Spatial", "Spatial_Table", "0",
                 "Spatial_Table", "is_merging"),
    os.path.join(asterixdb, "data", "red", "storage", "partition_0", "Level_Spatial", "Spatial_Table", "0",
                 "rtreeidx", "is_flushing"),
    os.path.join(asterixdb, "data", "red", "storage", "partition_0", "Level_Spatial", "Spatial_Table", "0",
                 "rtreeidx", "is_merging")
)


def wait_io():
    def is_doing_io():
        for f in io_flags:
            if os.path.isfile(f):
                return True
        return False

    while is_doing_io():
        print("I/O pending...")
        time.sleep(10)


def write_err(msg):
    err_log = open(os.path.join(logs_dir, task_name + ".err"), "a")
    if msg.endswith("\n"):
        err_log.write(msg)
    else:
        err_log.write(msg + "\n")
    err_log.close()


def parseline(line, kw):
    if kw in line:
        return line[line.index(kw) + len(kw) + 1:].replace("\n", "").split("\t")
    else:
        return []


def count_levels(components):
    lvs = set()
    for c in components:
        lvs.add(int(c.split("_")[0]))
    return len(lvs)


def extract_logs():
    with open(os.path.join(logs_dir, task_name + ".load.tmp1"), "w") as loadtmpf, \
            open(os.path.join(logs_dir, task_name + ".tables.tmp"), "w") as tablestmpf, \
            open(os.path.join(logs_dir, task_name + ".read.tmp"), "w") as readtmpf:
        for logp in glob.glob(os.path.join(asterixdb, "logs", "nc-red*")):
            with (open(logp, "r") if logp.endswith(".log") else gzip.open(logp, "rt")) as logf:
                is_err = False
                for line in logf:
                    if is_err:
                        if " WARN " in line or " INFO " in line or "\tWARN\t" in line or "\tINFO\t" in line:
                            is_err = False
                    else:
                        if " ERROR " in line or "\tERROR\t" in line:
                            is_err = True
                    if not is_err:
                        if "[FLUSH]\trtreeidx" in line:
                            parts = parseline(line, "[FLUSH]\trtreeidx")
                            if len(parts) != 7:
                                continue
                            timestamp = parts[0]
                            new_component = parts[6]
                            c_name = new_component.split(":")[0].replace("_", "\t")
                            loadtmpf.write(timestamp + "\tF\t" + "\t".join(parts[1:]) + "\n")
                            tablestmpf.write(c_name + "\t" + new_component.replace(":", "\t") + "\n")
                        elif "[MERGE]\trtreeidx" in line:
                            parts = parseline(line, "[MERGE]\trtreeidx")
                            if len(parts) != 8:
                                continue
                            timestamp = parts[0]
                            new_components = parts[7].split(";")
                            loadtmpf.write(timestamp + "\tM\t" + "\t".join(parts[1:]) + "\n")
                            for new_component in new_components:
                                c_name = new_component.split(":")[0].replace("_", "\t")
                                tablestmpf.write(c_name + "\t" + new_component.replace(":", "\t") + "\n")
                        elif "[COMPONENTS]\trtreeidx" in line:
                                parts = parseline(line, "[COMPONENTS]\trtreeidx")
                                if len(parts) != 2:
                                    continue
                                loadtmpf.write(parts[0] + "\tC\t" + parts[1] + "\n")
                        elif "[SEARCH]\trtreeidx" in line:
                                parts = parseline(line, "[SEARCH]\trtreeidx")
                                if len(parts) != 8:
                                    continue
                                readtmpf.write("\t".join(parts) + "\n")
                        else:
                            continue
            logf.close()
    loadtmpf.close()
    tablestmpf.close()
    readtmpf.close()
    call("sort -n -k1,1 \"{0}.tmp1\" |  cut -f2- > \"{0}.tmp2\""
         .format(os.path.join(logs_dir, task_name + ".load")), shell=True)
    try:
        os.remove(os.path.join(logs_dir, task_name + ".load.tmp1"))
    except:
        pass
    call("sort -n -k1,1 -k2,2 \"{0}.tmp\" |  cut -f3- > \"{0}.log\""
         .format(os.path.join(logs_dir, task_name + ".tables")), shell=True)
    try:
        os.remove(os.path.join(logs_dir, task_name + ".tables.tmp"))
    except:
        pass
    call("sort -n -k1,1 \"{0}.tmp\" |  cut -f2- > \"{0}.log\""
         .format(os.path.join(logs_dir, task_name + ".read")), shell=True)
    try:
        os.remove(os.path.join(logs_dir, task_name + ".read.tmp"))
    except:
        pass
    total_flushed = []
    total_merged = []
    tmp_space = []
    if is_level:
        num_levels = []
        with open(os.path.join(logs_dir, task_name + ".load.tmp2"), "r") as inf:
            for line in inf:
                parts = line.replace("\n", "").split("\t")
                if len(parts) < 2:
                    continue
                if parts[0] == "F":
                    flushed = int(parts[3])
                    merged = int(parts[4])
                    total_flushed.append(flushed)
                    total_merged.append(merged)
                    tmp_space.append(0)
                    if len(num_levels) == 0:
                        num_levels.append(1)
                    else:
                        num_levels.append(num_levels[-1])
                elif parts[0] == "M":
                    f_cnt = int(parts[1])
                    merged = int(parts[4])
                    new_components = parts[7].split(";")
                    new_size = 0
                    for c in new_components:
                        new_size += int(c.split(":")[1])
                    total_merged[f_cnt - 1] = merged
                    tmp_space[f_cnt - 1] = max(tmp_space[f_cnt - 1], new_size)
                elif parts[0] == "C":
                    components = parts[1].split(";")
                    f_cnt = int(components[0].split("_")[1])
                    if f_cnt > len(num_levels):
                        num_levels.append(count_levels(components))
                    else:
                        num_levels[f_cnt - 1] = count_levels(components)
                else:
                    continue
        inf.close()
        try:
            os.remove(os.path.join(logs_dir, task_name + ".load.tmp2"))
        except:
            pass
        with open(os.path.join(logs_dir, task_name + ".load.log"), "w") as outf:
            for i in range(len(total_merged)):
                outf.write("{0}\t{1}\t{2}\t{3}\n".format(total_flushed[i], total_merged[i], num_levels[i], tmp_space[i]))
        outf.close()
    else:
        stack_size = []
        with open(os.path.join(logs_dir, task_name + ".load.tmp2"), "r") as inf:
            for line in inf:
                parts = line.replace("\n", "").split("\t")
                if len(parts) < 2:
                    continue
                if parts[0] == "F":
                    flushed = int(parts[3])
                    merged = int(parts[4])
                    total_flushed.append(flushed)
                    total_merged.append(merged)
                    tmp_space.append(0)
                    if len(stack_size) == 0:
                        stack_size.append(1)
                    else:
                        stack_size.append(stack_size[-1] + 1)
                elif parts[0] == "M":
                    f_cnt = int(parts[1])
                    merged = int(parts[4])
                    new_components = parts[7].split(";")
                    new_size = 0
                    for c in new_components:
                        new_size += int(c.split(":")[1])
                    total_merged[f_cnt - 1] = merged
                    tmp_space[f_cnt - 1] = max(tmp_space[f_cnt - 1], new_size)
                elif parts[0] == "C":
                    components = parts[1].split(";")
                    f_cnt = int(components[0].split("_")[1])
                    if f_cnt > len(stack_size):
                        stack_size.append(len(components))
                    else:
                        stack_size[f_cnt - 1] = len(components)
                else:
                    continue
        inf.close()
        try:
            os.remove(os.path.join(logs_dir, task_name + ".load.tmp2"))
        except:
            pass
        with open(os.path.join(logs_dir, task_name + ".load.log"), "w") as outf:
            for i in range(len(total_merged)):
                outf.write(
                    "{0}\t{1}\t{2}\t{3}\n".format(total_flushed[i], total_merged[i], stack_size[i], tmp_space[i]))
        outf.close()


def zip_logs():
    in_files = [
        os.path.join(logs_dir, task_name + ".tables.log"),
        os.path.join(logs_dir, task_name + ".load.log"),
        os.path.join(logs_dir, task_name + ".read.log"),
        os.path.join(logs_dir, task_name + ".err")
    ]
    with zipfile.ZipFile(os.path.join(logs_dir, task_name + ".zip"), "w") as z:
        for f in in_files:
            if os.path.isfile(f) and os.path.getsize(f) > 0:
                z.write(f, os.path.basename(f), zipfile.ZIP_DEFLATED)
    z.close()
    for f in in_files:
        if os.path.isfile(f):
            os.remove(f)


wait_io()
try:
    os.remove(os.path.join(logs_dir, task_name + ".err"))
except:
    pass
extract_logs()
zip_logs()
