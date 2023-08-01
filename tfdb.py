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


def ip_within_sql(needle, haystack):
    if needle is None or haystack is None:
        return False
    needle = ipaddress.ip_address(needle)
    if "-" in haystack:
        r0, r1 = map(ipaddress.ip_address, haystack.split("-"))
        return needle >= r0 and needle <= r1
    elif "/" in haystack:
        haystack = ipaddress.ip_network(haystack)
        return needle in haystack
    else:
        pprint.pprint(f"cannot parse network {haystack}")
        raise Exception(f"cannot parse network {haystack}")


def ip_sortable_sql(ip_range_s):
    if ip_range_s is None:
        return False
    ip_address = None
    if "/" in ip_range_s:
        ip_range = ipaddress.ip_network(ip_range_s)
        ip_address = ip_range.network_address
    else:
        ip_address = ipaddress.ip_address(ip_range_s)

    return "{:#x}".format(ip_address)


def ip_truncate_sql(addr_as_text, bits):
    addr = ipaddress.ip_address(addr_as_text)
    addr_b = bytearray(addr.packed)
    assert bits % 8 == 0
    if bits <= 24:
        addr_b[3] = 0
    if bits <= 16:
        addr_b[2] = 0
    if bits <= 8:
        addr_b[1] = 0
    return str(ipaddress.ip_address(bytes(addr_b)))


def aws_account_sql(arn):
    return arn_field_sql(arn, 4)


def arn_field_sql(arn, fieldno):
    if arn is None:
        return False
    if not (arn.startswith("arn:aws:")):
        raise Exception(f'not an ARN: "{arn}"')
    parts = arn.split(":")
    return parts[fieldno]


def flowcache_rows(filename):
    with gzip.open(filename, mode="rb") as f:
        flow_keys = pickle.load(f)
        while True:
            try:
                k, v = pickle.load(f)
                keys = k.split(" ")
                row = {"bytes": v}
                for idx, i in enumerate(flow_keys):
                    row[i] = keys[idx]
                yield row
            except EOFError:
                break


class TerraformState:
    def __init__(self, db=None):
        if db is None:
            self.db = sqlite3.connect(":memory:")
        else:
            self.db = db
        self.db.create_function("ip_within", 2, ip_within_sql)
        self.db.create_function("ip_sortable", 1, ip_sortable_sql)
        self.db.create_function("ip_truncate", 2, ip_truncate_sql)
        self.db.create_function("aws_account", 1, aws_account_sql)
        self.db.create_function("arn_field", 2, arn_field_sql)
        self.cur = self.db.cursor()
        self.types = dict()
        self.ids = set()
        if db is None:
            self._exec("create table schema (tbl_name, col_name);")

    def _exec(self, statement, *params):
        try:
            self.db.execute(statement, *params)
        except:
            pprint.pprint([statement, params])
            raise
        self.db.commit()

    def _add(self, rtype, rdict):
        if rtype not in self.types:
            self._create_type(rtype, rdict.keys())
            self.types[rtype] = set(rdict.keys())
        if not (rdict.keys() <= self.types[rtype]):
            self._add_columns(rtype, list(rdict.keys() - self.types[rtype]))
            self.types[rtype].update(rdict.keys())

        def sqlize_data(z):
            if isinstance(z, str):
                return

        self._exec(
            f"""insert into {rtype}
        ({','.join(rdict.keys())})
        values ({','.join('?' * len(rdict))});""",
            list(rdict.values()),
        )

    def _create_type(self, rtype, rkeys):
        self._exec(f'create table {rtype} ({",".join(sorted(rkeys))});')
        for col in sorted(rkeys):
            self._exec("insert into schema values(?,?);", (rtype, col))

    def _add_columns(self, rtype, cols):
        for col in cols:
            self._exec(f"alter table {rtype} add column {col};")
            self._exec("insert into schema values(?,?);", (rtype, col))

    def add_state_file(self, state_file):
        tfstate = json.load(state_file)
        assert len(tfstate["modules"]) == 1
        f_resources = tfstate["modules"][0]["resources"]
        for name, resource in f_resources.items():
            # we want ID
            id = resource["primary"]["id"]
            rtype = resource["type"]
            r = {}

            class Pointer:
                def __init__(self, p=None):
                    self.p = p

            ra = dict(resource["primary"]["attributes"])
            arrays = Pointer({})
            for k in list(sorted(ra.keys())):  # '#' < '0'
                v = ra[k]
                by_dots = k.split(".")
                if len(by_dots) == 1:
                    continue
                del ra[k]  # remove arrays
                arr_host = arrays
                while len(by_dots) > 0:
                    x = by_dots.pop(0)
                    remaining = len(by_dots)
                    if x == "#":
                        assert arr_host.p is None
                        assert remaining == 0
                        arr_host.p = [Pointer() for i in range(0, int(v))]
                        break
                    # if remaining > 0:

                    if type(arr_host.p) == type(None):
                        # then host not an array, so must be hash time
                        arr_host.p = {}

                    if type(arr_host.p) == dict:
                        if x not in arr_host.p:
                            arr_host.p[x] = Pointer()
                        arr_host = arr_host.p[x]
                    elif type(arr_host.p) == list:
                        arr_host = arr_host.p[int(x)]
                    else:
                        assert False

                    if remaining == 0:
                        arr_host.p = v
            # depointerise
            arrays_nop = {}

            def depointerise(x):
                if type(x) == Pointer:
                    return depointerise(x.p)
                elif type(x) == list:
                    return [depointerise(i) for i in x]
                elif type(x) == dict:
                    return {k: depointerise(v) for k, v in x.items()}
                return x

            for k, v in depointerise(arrays).items():
                ra[k] = json.dumps(v)

            for k, v in ra.items():  # resource['primary']['attributes'].items():
                if k[-2:] in [".%", ".#"]:
                    continue
                k = re.sub(r"[^0-9a-zA-Z]", "_", k).lower()
                #                k = k.replace('.', '_').replace('-','_').replace(' ','_').replace(':','_').replace("\t", '_').replace("\xa0", '_')
                r[k] = v

            assert "id" in r
            # dedupe
            if rtype in self.types and "id" in r:
                # get existing
                self.cur.execute(f"select * from {rtype} where id=?;", (str(r["id"]),))
                rows = self.cur.fetchall()
                assert len(rows) < 2
                if len(rows) == 1:
                    # is this one better?
                    old_r = {}
                    new_r = {}
                    for k in r.keys():
                        old_r[k] = None
                        new_r[k] = r[k]
                    for idx, col_ in enumerate(self.cur.description):
                        col = col_[0]
                        old_r[col] = rows[0][idx]
                        if col not in new_r:
                            new_r[col] = None
                    if old_r == new_r:
                        continue
                    old_data_count = sum(
                        1 for _ in filter(lambda x: x is not None, old_r.values())
                    )
                    new_data_count = sum(
                        1 for _ in filter(lambda x: x is not None, new_r.values())
                    )
                    if new_data_count < old_data_count:
                        continue
                    self.db.execute(f"delete from {rtype} where id=?;", (r["id"],))
                    self.db.commit()

            self._add(rtype, r)

    def add_dict_of_tables(self, d_of_t):
        for t_name, t_rows in d_of_t.items():
            for r in t_rows:
                self._add(t_name, r)

    def add_flowsummary_file(self, flowcache_filename):
        for row in flowcache_rows(flowcache_filename):
            self._add("flow", row)

    def add_database_file(self, filename, database_name):
        self.db.execute(f"attach database ? as {database_name};", (filename,))
