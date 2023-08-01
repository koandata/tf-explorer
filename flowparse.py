from multiprocessing import Pool, cpu_count
import threading
import queue
from pprint import pprint
import sys
import os
import gzip
import csv
import time
import pickle
import argparse
import asyncio
import io
import subprocess
import sqlite3
import time

arg_parser = argparse.ArgumentParser(description="Parse and combine flow logs")
arg_parser.add_argument("flowdir", nargs="+", action="store")
arg_parser.add_argument("--summary-file", nargs="?")
arg_parser.add_argument("--sqlite-file", nargs="?")
arg_parser.add_argument("--summary-fields", nargs="?", default="src,dst")
arg_parser.add_argument("--flowcache", nargs="?", default="cache")
args = arg_parser.parse_args()

do_summary = (args.summary_file is not None) or (args.sqlite_file is not None)
summary_fields = args.summary_fields.split(",")
cache_root = args.flowcache

if args.sqlite_file:
    assert not os.path.exists(args.sqlite_file)

rkey_list = []
summary_keys = []
for idx, k in enumerate(["interface", "account", "src", "dst"]):
    if k in summary_fields:
        rkey_list.append("fields[%i]" % idx)
        summary_keys.append(k)

rkey_expr = (
    '"' + (" ".join(["%s"] * len(summary_fields))) + '" % (' + ",".join(rkey_list) + ")"
)


def simplify_row_key(k):
    assert False


exec(
    """
def simplify_row_key(k):
    fields = k.split(' ')
    return %s
"""
    % rkey_expr
)


def process_single_log(logfile, cache_file):
    assert logfile.endswith(".gz")

    tuple_dict = {}
    rows = 0
    with gzip.open(logfile, mode="rt") as f:
        headings = {}
        for idx, x in enumerate(f.readline().rstrip().split(" ")):
            headings[x] = idx
        interface_col = headings.get("interface-id", -1)
        account_col = headings["account-id"]
        src_col = headings["srcaddr"]
        dst_col = headings["dstaddr"]
        bytes_col = headings["bytes"]
        protocol_col = headings["protocol"]
        for line in f:
            row = line.rstrip().split(" ")
            #        for row in csv.DictReader(f, delimiter=' '):
            rows += 1
            try:
                src = row[src_col]
                if src == "-":
                    continue
                protocol = row[protocol_col]
                if protocol == "1":  # skip ICMP
                    continue
                dst = row[dst_col]
                key = "%s %s %s %s" % (
                    row[account_col],
                    row[interface_col] if interface_col != -1 else "0",
                    src,
                    dst,
                )
                bytes = int(row[bytes_col])
                if key in tuple_dict:
                    tuple_dict[key] += bytes
                else:
                    tuple_dict[key] = bytes
            except:
                pprint(row)
                raise

    with gzip.open(cache_file + ".tmp", mode="wb") as f:
        pickle.dump((tuple_dict, rows), f)
    os.rename(cache_file + ".tmp", cache_file)

    return cache_file


def combine_summary(master, addition):
    for k_, v in addition.items():
        k = simplify_row_key(k_)
        if k not in master:
            master[k] = v
        else:
            master[k] += v


results_lock = threading.Lock()


def add_result(results, k, v):
    with results_lock:
        results[k] = v


def check_for_results(results, q):
    last_result = None
    with results_lock:
        done = []
        for cache_file, i in results.items():
            if i.ready():
                _result = i.get()
                assert _result == cache_file

                if do_summary:
                    q.put(cache_file)
                done.append(cache_file)
            else:
                last_result = i
        for cache_file in done:
            del results[cache_file]

    return last_result, len(done)


def combine_folder(folder_cache_file, cache_files):
    folder_combined = {}
    folder_rows = 0
    dict_rows = 0
    for cache_file in cache_files:
        with gzip.open(cache_file, mode="rb") as f:
            file_data = pickle.load(f)
        tuple_dict, rows = file_data
        dict_rows += len(tuple_dict)
        folder_rows += rows
        for k, v in tuple_dict.items():
            if k not in folder_combined:
                folder_combined[k] = v
            else:
                folder_combined[k] += v

    with gzip.open(folder_cache_file + ".tmp", mode="wb") as f:
        pickle.dump((folder_combined, folder_rows), f)
    os.rename(folder_cache_file + ".tmp", folder_cache_file)
    pprint(("combine_folder", folder_cache_file, dict_rows, len(folder_combined)))
    return folder_cache_file


def combine_worker(results, combine_q):

    total_rows = 0
    start_time = time.time()
    last_status = start_time
    total_q = 0

    while True:
        item = combine_q.get()

        if item is False:  # sentinel, end
            combine_q.task_done()
            return

        with gzip.open(item, mode="rb") as f:
            combine_data = pickle.load(f)
        tuple_dict, rows = combine_data

        combine_summary(summary, tuple_dict)

        total_q += 1

        total_rows += rows

        combine_q.task_done()
        check_for_results(results, combine_q)
        time.sleep(0)
        if (time.time() - last_status) > 0.8:
            last_status = time.time()
            how_long = last_status - start_time
            pprint(
                (
                    total_rows,
                    total_rows / how_long,
                    total_q / how_long,
                    len(results),
                    combine_q.qsize(),
                )
            )


if __name__ == "__main__":  # not multiprocess
    summary = {}
    results = {}
    to_combine = queue.Queue()
    combine_thread = threading.Thread(target=combine_worker, args=(results, to_combine))
    combine_thread.start()

    with Pool(cpu_count() - 4) as pool:
        running = 0
        for flowdir in args.flowdir:
            for folder, subfolders, files in os.walk(flowdir, topdown=False):
                cached = []
                uncached = False
                folder_cache_file = os.path.join(cache_root, folder + ".folder")
                for logfile in map(lambda x: os.path.join(folder, x), files):
                    cache_file = os.path.join(cache_root, logfile)
                    cache_folder = os.path.dirname(cache_file)

                    if os.path.exists(cache_file):
                        cached.append(cache_file)
                    elif logfile.endswith(".gz"):
                        uncached = True
                        # invalidate folder cache
                        if os.path.exists(folder_cache_file):
                            pprint(("invalidate folder cache", folder_cache_file))
                            os.remove(folder_cache_file)
                        if not os.path.exists(cache_folder):
                            os.makedirs(os.path.join(cache_folder))
                        add_result(
                            results,
                            cache_file,
                            pool.apply_async(process_single_log, (logfile, cache_file)),
                        )
                        running += 1
                        if running > 10:
                            check_for_results(results, to_combine)
                            running = 0
                if len(cached) > 0 and uncached is False:
                    # candidate for folder_cache:
                    if not os.path.exists(folder_cache_file):
                        add_result(
                            results,
                            folder_cache_file,
                            pool.apply_async(
                                combine_folder, (folder_cache_file, cached)
                            ),
                        )
                        running += 1
                        if running > 10:
                            check_for_results(results, to_combine)
                            running = 0
                    else:
                        to_combine.put(folder_cache_file)
                else:
                    if do_summary:
                        for cache_file in cached:
                            to_combine.put(cache_file)

        while len(results) > 0:
            last_result, items_done = check_for_results(results, to_combine)
            if items_done == 0:
                last_result.wait(0.1)
        to_combine.put(False)
        combine_thread.join()
        assert to_combine.empty()

if args.summary_file:
    start_summary = time.time()
    with gzip.open(args.summary_file + ".tmp", mode="wb") as f:
        pickle.dump(summary_keys, f)
        for k in sorted(summary.keys()):
            pickle.dump((k, summary[k]), f)
    os.rename(args.summary_file + ".tmp", args.summary_file)
    pprint(("summary written in ", time.time() - start_summary))

if args.sqlite_file:
    start_sqlite = time.time()
    db = sqlite3.connect(args.sqlite_file)
    cur = db.cursor()
    cols = ",".join(summary_keys + ["bytes"])
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA synchronous = 0;")
    db.execute(f"create table flow ({cols});")
    for z in summary_keys:
        db.execute(f"create index if not exists {z}_index on flow ({z});")

    stmt = f"""insert into flow ({cols})
values ({','.join('?' * len(summary_keys))},?)"""
    db.commit()
    row_count = 0
    batch = []

    for k, v in summary.items():
        row = k.split(" ")
        row.append(v)

        batch.append(row)

        if len(batch) > 100:
            db.executemany(stmt, batch)
            db.commit()
            batch = []

    if len(batch) > 0:
        db.executemany(stmt, batch)
        db.commit()
    pprint(("sqlite written in ", time.time() - start_sqlite))
