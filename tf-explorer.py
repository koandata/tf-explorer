import argparse
import pprint
import atexit
import os
import json
import sqlite3
import prettytable
import readline
import ipaddress
import glob
import gzip
import pickle
import re
import yaml
from tfdb import TerraformState

arg_parser = argparse.ArgumentParser(description="Analyse terraform state")
arg_parser.add_argument("state", nargs="*")
arg_parser.add_argument("--json", type=argparse.FileType("r"), nargs="*")
arg_parser.add_argument("--yaml", type=argparse.FileType("r"), nargs="*")
arg_parser.add_argument("--sqlite", nargs="*")
arg_parser.add_argument("--flowsummary", nargs="*")
arg_parser.add_argument("--flowdb", nargs="?")
args = arg_parser.parse_args()

sqlite3.enable_callback_tracebacks(True)

db = None
if args.sqlite:
    assert len(args.sqlite) == 1
    assert not args.state
    #    assert not args.json
    #    assert not args.json
    assert not args.flowsummary

    db = sqlite3.connect(args.sqlite[0])

tfs = TerraformState(db=db)

for state_file_name in args.state:
    with open(state_file_name, "r") as state_file:
        tfs.add_state_file(state_file)

if args.json is not None:
    for json_file in args.json:
        tfs.add_dict_of_tables(json.load(json_file))

if args.yaml is not None:
    for yaml_file in args.yaml:
        tfs.add_dict_of_tables(yaml.safe_load(yaml_file))

if args.flowsummary is not None:
    for flowsummary_file in args.flowsummary:
        tfs.add_flowsummary_file(flowsummary_file)
if args.flowdb is not None:
    tfs.add_database_file(args.flowdb, "flowdb")


histfile = os.path.join(os.path.expanduser("~"), ".tf-explorer_history")
try:
    readline.read_history_file(histfile)
    readline.set_history_length(1000)
except FileNotFoundError:
    pass

atexit.register(readline.write_history_file, histfile)

while True:
    line = input("> ")
    flags = set()
    while line.startswith("#") and " " in line:
        space = line.find(" ")
        flags.add(line[1:space])
        line = line[space + 1 :]
    sql = line
    if line.startswith("."):
        cmd = line.split(" ")
        flags.add("no-format")
        if cmd[0] == ".tab":
            sql = "select name from sqlite_master where type = 'table';"
        elif cmd[0] == ".type":
            sql = "select name from sqlite_master where type = 'table';"
        elif cmd[0] == ".schema":
            if len(cmd) == 1:
                sql = "select sql from sqlite_master where type='table';"
            else:
                sql = f"""  
select sql from sqlite_master where type='table' and name in 
({','.join(map(lambda s: "'%s'" % s, cmd[1:]))});"""
        elif cmd[0] == ".cols":
            flags.remove("no-format")
            sql = f"""  
select * from schema where tbl_name in 
({','.join(map(lambda s: "'%s'" % s, cmd[1:]))});"""
        elif cmd[0] == ".loop":  # .loop query_table data_table
            sql = f"""select * from {cmd[1]}"""
            flags.remove("no-format")
            flags.add("loop")
        else:
            sql = "select 'unknown command';"

    cur = tfs.db.cursor()
    try:
        cur.execute(sql)
        if "no-format" in flags:
            for i in cur.fetchall():
                print(i[0])
        elif "loop" in flags:
            colmap = {}
            for idx, i in enumerate(cur.description):
                colmap[i[0]] = idx
            queries = []
            for row in cur.fetchall():
                q = {}
                for k, idx in colmap.items():
                    q[k] = row[idx]
                queries.append(q)
            cur.execute(f""" select * from {cmd[2]}""")
            colmap = {}
            for idx, i in enumerate(cur.description):
                colmap[i[0]] = idx
            result_rows = []
            for row_ in cur.fetchall():
                row = {}
                for k, idx in colmap.items():
                    row[k] = row_[idx]
                rowout = []
                result_rows.append(rowout)
                for q in queries:
                    qtext = q["q"]
                    params = []
                    for p in range(0, 10):
                        p_k = f"""p{p}"""
                        if p_k in q and q[p_k] is not None:
                            params.append(row[q[p_k]])
                    cur.execute(qtext, params)
                    result = cur.fetchone()
                    if result is not None:
                        result = result[0]
                    rowout.append(str(result))
                print(",".join(rowout))
        else:
            fields = []
            for i in map(lambda z: z[0], cur.description):
                if i not in fields:
                    fields.append(i)
                else:
                    for j in range(1, 20):
                        f_n = f"{i}_{j}"
                        if f_n not in fields:
                            fields.append(f_n)
                            break

            rows = cur.fetchall()
            table = prettytable.PrettyTable()

            table.field_names = fields
            table.add_rows(rows)
            if "collapse" in flags:  # hide all columns which are only NULL
                not_nones = [False] * len(fields)
                for i in range(0, len(fields)):
                    for j in rows:
                        if j[i] is not None:
                            not_nones[i] = True
                            break

                for i in range(len(fields) - 1, -1, -1):
                    if not not_nones[i]:
                        table.del_column(fields[i])

            table.align = "l"
            if "md" in flags:
                table.set_style(prettytable.MARKDOWN)
            print(table)

    except sqlite3.OperationalError as e:
        pprint.pprint(e)
