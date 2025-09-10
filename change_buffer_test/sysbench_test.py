#!/usr/bin/env python3
import argparse
import datetime as dt
import os
import pathlib
import subprocess
import threading
import time
import re
import platform
import json
import shutil
import socket
from shlex import quote
import shlex
import csv
import copy

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_SCENARIOS = {
    "default_ibuf_3": REPO_ROOT / "configs" / "default_ibuf_3.json",
}

DEFAULTS = dict(
    host="127.0.0.1",
    port=34512,
    user="sbtest",
    password="sbtest",
    database="sbtest",
    tables=8,
    table_size=10000000,
    threads=32,
    duration=28800,
    report_interval=10,
    status_interval=10,
    metrics_interval=10,
    out_dir="/nvme/matias.sanchez/sandboxes/stats/results",
)

def _sanitize_name(name: str) -> str:
    name = name.strip()
    has_bad = re.search(r"[^A-Za-z0-9._\- ]", name)
    name = re.sub(r"[^A-Za-z0-9._\- ]", "", name)
    if has_bad:
        name = name.replace(" ", "")
    else:
        name = name.replace(" ", "_")
    if not name:
        name = dt.datetime.now().strftime("test_%Y%m%d_%H%M%S")
    return name

def timestamp():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def which(cmd):
    try:
        subprocess.run(["which", cmd], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False

def run_cmd(cmd, **popen_kwargs):
    return subprocess.run(cmd, **popen_kwargs)

def mysql_exec(host, port, user, password, sql, out_path=None):
    cmd = [
        "mysql",
        f"-h{host}",
        f"-P{port}",
        f"-u{user}",
        f"-p{password}",
        "-e",
        sql,
    ]
    if out_path:
        with open(out_path, "a") as fh:
            return run_cmd(cmd, stdout=fh, stderr=fh)
    return run_cmd(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def mysql_query_text(host, port, user, password, sql) -> str:
    cmd = [
        "mysql",
        f"-h{host}", f"-P{port}", f"-u{user}", f"-p{password}",
        "-N", "-e", sql,
    ]
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError as e:
        return f"<mysql_query_error>\n{e.output}"


def extract_ibuf_section(status_text: str) -> str:
    try:
        m = re.search(
            r"INSERT BUFFER AND ADAPTIVE HASH INDEX\n[-]+\n(.*?)(?:\n\n[A-Z ].*\n[-]+|\n\nEND OF INNODB MONITOR OUTPUT)",
            status_text,
            re.S,
        )
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return status_text


def parse_ibuf_section(section: str):
    out = {
        'size': None,
        'seg_size': None,
        'free_list_len': None,
        'merges_insert': 0,
        'merges_delete_mark': 0,
        'merges_delete': 0,
        'discarded_insert': 0,
        'discarded_delete_mark': 0,
        'discarded_delete': 0,
    }
    m = re.search(r"Ibuf:\s*size\s*(\d+),\s*free list len\s*(\d+),\s*seg size\s*(\d+)", section)
    if m:
        out['size'] = int(m.group(1))
        out['free_list_len'] = int(m.group(2))
        out['seg_size'] = int(m.group(3))
    m = re.search(r"merged operations:\s*\n?\s*insert\s*(\d+),\s*delete mark\s*(\d+),\s*delete\s*(\d+)", section)
    if m:
        out['merges_insert'] = int(m.group(1))
        out['merges_delete_mark'] = int(m.group(2))
        out['merges_delete'] = int(m.group(3))
    m = re.search(r"discarded operations:\s*\n?\s*insert\s*(\d+),\s*delete mark\s*(\d+),\s*delete\s*(\d+)", section)
    if m:
        out['discarded_insert'] = int(m.group(1))
        out['discarded_delete_mark'] = int(m.group(2))
        out['discarded_delete'] = int(m.group(3))
    return out

def _cmd_output(cmd: list) -> str:
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except Exception as e:
        out = f"<error running {cmd}: {e}>"
    return out

def _write_and_print_header(args, sysbench_cmd_preview: str):
    info = {
        "timestamp": timestamp(),
        "host": socket.gethostname(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "paths": {
            "cwd": str(os.getcwd()),
            "PATH": os.environ.get("PATH", ""),
            "sysbench_path": shutil.which("sysbench"),
            "mysql_path": shutil.which("mysql"),
        },
        "binaries": {
            "sysbench_version": _cmd_output(["sysbench", "--version"]).strip(),
            "mysql_client_version": _cmd_output(["mysql", "--version"]).strip(),
        },
        "effective_args": {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "database": args.database,
            "tables": args.tables,
            "table_size": args.table_size,
            "threads": args.threads,
            "duration": args.duration,
            "report_interval": args.report_interval,
            "status_interval": args.status_interval,
            "out_dir": args.out_dir,
            "skip_prepare": getattr(args, "skip_prepare", False),
            "reset_db": getattr(args, "reset_db", False),
            "sysbench_script": getattr(args, "sysbench_script", "oltp_write_only"),
            "sb_extra": getattr(args, "sb_extra", ""),
            "metrics_interval": getattr(args, "metrics_interval", args.status_interval),
            "phase": getattr(args, "phase", "run"),
            "create_extra_indexes": getattr(args, "create_extra_indexes", False),
            "target_gb": getattr(args, "target_gb", None),
            "est_bytes_per_row": getattr(args, "est_bytes_per_row", None),
        },
        "logs": {
            "prepare": str(args.log_prepare),
            "run": str(args.log_run),
            "status": str(args.log_status),
            "manifest": str(args.log_manifest),
            "variables_snapshot": str(args.log_vars),
            "innodb_metrics": str(args.log_ibuf_metrics),
            "innodb_status": str(args.log_innodb_status),
            "innodb_metrics_csv": str(args.csv_ibuf_metrics),
            "innodb_status_csv": str(args.csv_innodb_status),
        },
        "sysbench_command_preview": sysbench_cmd_preview,
    }

    if getattr(args, 'target_gb', None) is not None:
        info.setdefault('notes', {})['table_size_source'] = 'computed_from_target_gb'
    print(f"[{timestamp()}] === Test Environment & Effective Parameters ===")
    for k, v in info["effective_args"].items():
        print(f"  {k}: {v}")
    print(f"  sysbench: {info['binaries']['sysbench_version']}")
    print(f"  mysql:    {info['binaries']['mysql_client_version']}")
    print(f"  logs:     {info['logs']}")

    with args.log_manifest.open("w") as mf:
        json.dump(info, mf, indent=2, sort_keys=True)

def status_sampler(stop_evt, args):
    hdr_written = False
    while not stop_evt.is_set():
        start = time.time()
        line = f"# {timestamp()}  SHOW GLOBAL STATUS\n"
        try:
            with args.log_status.open("a") as fh:
                fh.write(line)
            mysql_exec(
                args.host,
                args.port,
                args.user,
                args.password,
                "SHOW GLOBAL STATUS",
                out_path=str(args.log_status),
            )
            with args.log_status.open("a") as fh:
                fh.write("\n")
        except Exception as e:
            with args.log_status.open("a") as fh:
                fh.write(f"# {timestamp()}  ERROR collecting status: {e}\n\n")
        elapsed = time.time() - start
        to_sleep = max(0.0, args.status_interval - elapsed)
        stop_evt.wait(to_sleep)


def _popen_to_file(cmd, logfile: pathlib.Path):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    def _pump():
        with logfile.open("a") as f:
            for line in proc.stdout:
                f.write(line)
                f.flush()
    t = threading.Thread(target=_pump, daemon=True)
    t.start()
    return proc

def build_sysbench_base(args):
    return [
        "sysbench",
        f"{args.sysbench_script}",
        "--db-driver=mysql",
        f"--mysql-host={args.host}",
        f"--mysql-port={args.port}",
        f"--mysql-user={args.user}",
        f"--mysql-password={args.password}",
        f"--mysql-db={args.database}",
        f"--tables={args.tables}",
    ]

def ensure_outdir_and_logs(args):
    pathlib.Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    args.log_prepare  = pathlib.Path(args.out_dir) / f"sysbench_prepare_{stamp}.log"
    args.log_run      = pathlib.Path(args.out_dir) / f"sysbench_run_{stamp}.log"
    args.log_status   = pathlib.Path(args.out_dir) / f"global_status_{stamp}.log"
    args.log_manifest = pathlib.Path(args.out_dir) / f"manifest_{stamp}.json"
    args.log_vars     = pathlib.Path(args.out_dir) / f"mysql_variables_{stamp}.log"
    args.log_ibuf_metrics = pathlib.Path(args.out_dir) / f"innodb_metrics_{stamp}.log"
    args.log_innodb_status = pathlib.Path(args.out_dir) / f"innodb_status_{stamp}.log"
    args.csv_ibuf_metrics  = pathlib.Path(args.out_dir) / f"innodb_metrics_{stamp}.csv"
    args.csv_innodb_status = pathlib.Path(args.out_dir) / f"innodb_status_{stamp}.csv"

def sysbench_prepare(args):
    cmd = build_sysbench_base(args) + [
        f"--table-size={args.table_size}",
        "--threads=8",
        f"--report-interval={args.report_interval}",
    ] + (shlex.split(getattr(args, "sb_extra", "")) if getattr(args, "sb_extra", "") else []) + [
        "--create_secondary=on",
        "--mysql-storage-engine=InnoDB",
        "prepare",
    ]
    proc = _popen_to_file(cmd, args.log_prepare)
    proc.wait()
    return subprocess.CompletedProcess(cmd, returncode=proc.returncode)

def sysbench_run(args):
    cmd = build_sysbench_base(args) + [
        f"--threads={args.threads}",
        f"--time={args.duration}",
        f"--report-interval={args.report_interval}",
    ] + (shlex.split(getattr(args, "sb_extra", "")) if getattr(args, "sb_extra", "") else []) + [
        "run",
    ]
    return _popen_to_file(cmd, args.log_run)

def maybe_create_db(args):
    sql = f"CREATE DATABASE IF NOT EXISTS `{args.database}`"
    mysql_exec(args.host, args.port, args.user, args.password, sql, out_path=str(args.log_prepare))

def estimate_bytes_per_row(extra_indexes_count: int) -> int:
    base = 200
    per_sec = 38
    bpr = int(base + per_sec * extra_indexes_count)
    return int(bpr * 1.2)


def apply_target_gb(args):
    if getattr(args, 'target_gb', None) is None:
        return
    extra_idx = 1
    if getattr(args, 'create_extra_indexes', False):
        extra_idx += 3
    args.est_bytes_per_row = estimate_bytes_per_row(extra_idx)
    target_bytes = int(float(args.target_gb) * (1024**3))
    total_rows = max(1, target_bytes // max(1, args.est_bytes_per_row))
    per_table = max(1, total_rows // args.tables)
    args.table_size = per_table

def _ensure_csv_header(path: pathlib.Path, header: list[str]):
    exists = path.exists()
    with path.open('a', newline='') as fh:
        w = csv.writer(fh)
        if not exists:
            w.writerow(header)


def ibuf_sampler(stop_evt, args):
    _ensure_csv_header(args.csv_ibuf_metrics, ['ts','name','count'])
    _ensure_csv_header(args.csv_innodb_status, ['ts','size','seg_size','free_list_len','merges_insert','merges_delete_mark','merges_delete','discarded_insert','discarded_delete_mark','discarded_delete'])
    while not stop_evt.is_set():
        start = time.time()
        ts = timestamp()
        try:
            txt = mysql_query_text(
                args.host, args.port, args.user, args.password,
                "SELECT NAME, COUNT FROM INFORMATION_SCHEMA.INNODB_METRICS WHERE NAME LIKE 'ibuf_%' ORDER BY NAME"
            )
            with args.log_ibuf_metrics.open("a") as fh:
                fh.write(f"# {ts}  INNODB_METRICS ibuf_*\n")
                fh.write(txt if txt.endswith("\n") else txt+"\n")
                fh.write("\n")
            with args.csv_ibuf_metrics.open('a', newline='') as fhc:
                w = csv.writer(fhc)
                for line in txt.strip().splitlines():
                    parts = line.split('\t')
                    if len(parts) == 2 and parts[1].strip().isdigit():
                        w.writerow([ts, parts[0], parts[1]])
        except Exception as e:
            with args.log_ibuf_metrics.open("a") as fh:
                fh.write(f"# {ts}  ERROR collecting INNODB_METRICS: {e}\n\n")
        try:
            full = mysql_query_text(
                args.host, args.port, args.user, args.password,
                "SHOW ENGINE INNODB STATUS"
            )
            section = extract_ibuf_section(full)
            with args.log_innodb_status.open("a") as fh:
                fh.write(f"# {ts}  SHOW ENGINE INNODB STATUS (INSERT BUFFER section)\n")
                fh.write(section + ("\n" if not section.endswith("\n") else ""))
                fh.write("\n\n")
            parsed = parse_ibuf_section(section)
            with args.csv_innodb_status.open('a', newline='') as fhc:
                w = csv.writer(fhc)
                w.writerow([
                    ts,
                    parsed.get('size'), parsed.get('seg_size'), parsed.get('free_list_len'),
                    parsed.get('merges_insert'), parsed.get('merges_delete_mark'), parsed.get('merges_delete'),
                    parsed.get('discarded_insert'), parsed.get('discarded_delete_mark'), parsed.get('discarded_delete'),
                ])
        except Exception as e:
            with args.log_innodb_status.open("a") as fh:
                fh.write(f"# {ts}  ERROR collecting SHOW ENGINE INNODB STATUS: {e}\n\n")
        elapsed = time.time() - start
        to_sleep = max(0.0, args.metrics_interval - elapsed)
        stop_evt.wait(to_sleep)


def update_my_cnf(params: dict, my_cnf_path: str):
    path = pathlib.Path(my_cnf_path)
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        lines = []
    out_lines = []
    in_mysqld = False
    handled = set()
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('['):
            if in_mysqld:
                for k, v in params.items():
                    if k not in handled:
                        out_lines.append(f"{k}={v}")
                        handled.add(k)
            in_mysqld = stripped.lower() == '[mysqld]'
            out_lines.append(line)
            continue
        if in_mysqld:
            key = stripped.split('=')[0].strip() if '=' in stripped else None
            if key in params:
                out_lines.append(f"{key}={params[key]}")
                handled.add(key)
            else:
                out_lines.append(line)
        else:
            out_lines.append(line)
    if in_mysqld:
        for k, v in params.items():
            if k not in handled:
                out_lines.append(f"{k}={v}")
                handled.add(k)
    elif params:
        out_lines.append('[mysqld]')
        for k, v in params.items():
            out_lines.append(f"{k}={v}")
    path.write_text("\n".join(out_lines) + "\n")


def restart_mysql():
    candidates = [
        ["systemctl", "restart", "mysqld"],
        ["systemctl", "restart", "mysql"],
        ["brew", "services", "restart", "mysql"],
        ["brew", "services", "restart", "mysql@8.0"],
        ["mysql.server", "restart"],
    ]
    last_rc = None
    for cmd in candidates:
        try:
            rc = run_cmd(cmd)
            last_rc = rc.returncode
            if rc.returncode == 0:
                return
        except Exception:
            continue
    raise SystemExit(f"mysql restart failed using known methods (last rc={last_rc}). Please restart the service manually.")


def parse_sysbench_log(path: pathlib.Path):
    tps = None
    p95 = None
    try:
        with path.open() as fh:
            for line in fh:
                m = re.search(r"transactions:\s*\d+\s*\(([^ ]+) per sec\.\)", line)
                if m:
                    try:
                        tps = float(m.group(1))
                    except ValueError:
                        pass
                m = re.search(r"95th percentile:\s*([0-9.]+)", line)
                if m:
                    try:
                        p95 = float(m.group(1))
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass
    return tps, p95


def compute_ibuf_rates(csv_path: pathlib.Path, duration: int):
    metrics = [
        'ibuf_merges_insert',
        'ibuf_merges_delete_mark',
        'ibuf_merges_delete',
        'ibuf_discarded_insert',
        'ibuf_discarded_delete_mark',
        'ibuf_discarded_delete',
    ]
    data = {m: [] for m in metrics}
    try:
        with csv_path.open() as fh:
            reader = csv.reader(fh)
            next(reader, None)
            for row in reader:
                if len(row) != 3:
                    continue
                _, name, cnt = row
                if name in data and cnt.isdigit():
                    data[name].append(int(cnt))
    except FileNotFoundError:
        pass
    rates = {}
    for m, values in data.items():
        if len(values) >= 2:
            rates[f"{m}_per_s"] = (values[-1] - values[0]) / max(1, duration)
        else:
            rates[f"{m}_per_s"] = 0.0
    return rates


def load_scenarios(path: pathlib.Path):
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        raise SystemExit(f"Scenario file not found: {path}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid JSON in scenario file {path}: {e}")

    if not isinstance(data, list):
        raise SystemExit("Scenario file must contain a JSON array")

    for idx, scen in enumerate(data, 1):
        if not isinstance(scen, dict):
            raise SystemExit(f"Scenario #{idx} is not an object")
        name = scen.get("name")
        if not isinstance(name, str):
            raise SystemExit(f"Scenario #{idx} missing 'name' string")
        params = scen.get("params")
        if not isinstance(params, dict):
            raise SystemExit(f"Scenario '{name}' missing 'params' object")
        sb = scen.get("sysbench")
        if not isinstance(sb, dict):
            raise SystemExit(f"Scenario '{name}' missing 'sysbench' object")
        for key in ("threads", "duration"):
            if not isinstance(sb.get(key), int):
                raise SystemExit(f"Scenario '{name}' sysbench.{key} must be an integer")
        for opt in ("script", "sb_extra"):
            if opt in sb and not isinstance(sb[opt], str):
                raise SystemExit(f"Scenario '{name}' sysbench.{opt} must be a string")

    return data


def run_single(args):
    if args.test_name:
        test_name = _sanitize_name(args.test_name)
    else:
        print(f"[{timestamp()}] Ready to start. A per-test folder will be created under base: {args.out_dir}")
        raw = input("Enter a descriptive test folder name (e.g., 'cb_all_comp_8idx_32thr'): ").strip()
        test_name = _sanitize_name(raw)
    args.out_dir = str(pathlib.Path(args.out_dir) / test_name)

    apply_target_gb(args)

    if not which("sysbench"):
        raise SystemExit("ERROR: 'sysbench' not found in PATH.")
    if not which("mysql"):
        raise SystemExit("ERROR: 'mysql' client not found in PATH.")

    ensure_outdir_and_logs(args)

    try:
        with args.log_vars.open("a") as cf:
            cf.write(f"# {timestamp()}  SHOW GLOBAL VARIABLES snapshot\n")
        mysql_exec(
            args.host,
            args.port,
            args.user,
            args.password,
            "SHOW GLOBAL VARIABLES",
            out_path=str(args.log_vars),
        )
        with args.log_vars.open("a") as cf:
            cf.write("\n")
    except Exception as e:
        with args.log_vars.open("a") as cf:
            cf.write(f"# {timestamp()}  ERROR collecting SHOW GLOBAL VARIABLES: {e}\n\n")

    try:
        mysql_exec(args.host, args.port, args.user, args.password,
                   "SET GLOBAL innodb_monitor_enable='all'",
                   out_path=str(args.log_vars))
        with args.log_vars.open("a") as cf:
            cf.write(f"# {timestamp()}  Enabled innodb_monitor_enable=all\n\n")
    except Exception as e:
        with args.log_vars.open("a") as cf:
            cf.write(f"# {timestamp()}  ERROR enabling innodb_monitor_enable=all: {e}\n\n")

    preview = " ".join([
        "sysbench", args.sysbench_script,
        "--db-driver=mysql",
        f"--mysql-host={args.host}",
        f"--mysql-port={args.port}",
        f"--mysql-user={args.user}",
        f"--mysql-db={args.database}",
        f"--tables={args.tables}",
        f"--table-size={args.table_size}",
        f"--threads={args.threads}",
        f"--time={args.duration}",
        f"--report-interval={args.report_interval}",
        (args.sb_extra or "").strip()
    ])
    _write_and_print_header(args, preview)

    print(f"[{timestamp()}] Using test folder: {args.out_dir}")
    print(f"[{timestamp()}] Logs:")
    print(f"  prepare -> {args.log_prepare}")
    print(f"  run     -> {args.log_run}")
    print(f"  status  -> {args.log_status}")
    print(f"  ibuf metrics -> {args.log_ibuf_metrics}")
    print(f"  innodb status -> {args.log_innodb_status}")
    print(f"  metrics CSV -> {args.csv_ibuf_metrics}")
    print(f"  status  CSV -> {args.csv_innodb_status}")
    if args.target_gb is not None:
        print(f"  NOTE: --target-gb set to {args.target_gb}. table-size computed as {args.table_size} rows/table across {args.tables} tables.")

    if args.phase in ("init","both"):
        if args.reset_db:
            mysql_exec(args.host, args.port, args.user, args.password,
                       f"DROP DATABASE IF EXISTS `{args.database}`",
                       out_path=str(args.log_prepare))
            mysql_exec(args.host, args.port, args.user, args.password,
                       f"CREATE DATABASE `{args.database}`",
                       out_path=str(args.log_prepare))
        else:
            maybe_create_db(args)
        print(f"[{timestamp()}] Running sysbench PREPARE (table-size={args.table_size})…")
        rc = sysbench_prepare(args).returncode
        if rc != 0:
            print(f"[{timestamp()}] PREPARE returned non-zero ({rc}). Check {args.log_prepare}")
        if args.create_extra_indexes:
            print(f"[{timestamp()}] Adding extra secondary indexes on sbtest tables…")
            for i in range(1, args.tables + 1):
                sql = (
                    f"ALTER TABLE sbtest{i} "
                    f"ADD INDEX idx_c (c(20)), "
                    f"ADD INDEX idx_pad (pad(20)), "
                    f"ADD INDEX idx_kid (k,id)"
                )
                mysql_exec(args.host, args.port, args.user, args.password, sql, out_path=str(args.log_prepare))
        if args.phase == "init":
            print(f"[{timestamp()}] Initialization completed. Data prepared in DB '{args.database}'.")
            print(f"[{timestamp()}] Done. Logs in: {args.out_dir}")
            return

    print(f"[{timestamp()}] Starting sysbench RUN for {args.duration}s with {args.threads} threads…")
    proc = sysbench_run(args)

    stop_evt = threading.Event()
    sampler_thr = threading.Thread(target=status_sampler, args=(stop_evt, args), daemon=True)
    sampler_thr.start()
    ibuf_thr = None
    if not args.disable_ibuf_sampler:
        ibuf_thr = threading.Thread(target=ibuf_sampler, args=(stop_evt, args), daemon=True)
        ibuf_thr.start()

    rc = proc.wait()
    stop_evt.set()
    sampler_thr.join(timeout=5)
    if ibuf_thr is not None:
        ibuf_thr.join(timeout=5)

    print(f"[{timestamp()}] sysbench finished with exit code {rc}")
    print(f"[{timestamp()}] Done. Logs in: {args.out_dir}")


def run_scenarios(args):
    base_dir = pathlib.Path(args.out_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    scen_path = DEFAULT_SCENARIOS.get(args.scenario_file, pathlib.Path(args.scenario_file))
    scenarios = load_scenarios(scen_path)
    summary = []
    for scen in scenarios:
        scen_name = _sanitize_name(scen.get('name', 'scenario'))
        params = scen.get('params', {})
        sb = scen.get('sysbench', {})
        update_my_cnf(params, args.my_cnf)
        restart_mysql()
        scen_args = copy.deepcopy(args)
        scen_args.test_name = scen_name
        scen_args.out_dir = str(base_dir)
        scen_args.threads = sb.get('threads', args.threads)
        scen_args.duration = sb.get('duration', args.duration)
        if 'script' in sb:
            scen_args.sysbench_script = sb['script']
        if 'sb_extra' in sb:
            scen_args.sb_extra = sb['sb_extra']
        run_single(scen_args)
        tps, p95 = parse_sysbench_log(scen_args.log_run)
        rates = compute_ibuf_rates(scen_args.csv_ibuf_metrics, scen_args.duration)
        row = {
            'scenario': scen_name,
            'threads': scen_args.threads,
            'duration': scen_args.duration,
            'tps_avg': tps,
            'p95_ms': p95,
        }
        row.update(rates)
        summary.append(row)
    if summary:
        summary_path = base_dir / 'summary.csv'
        with summary_path.open('w', newline='') as fh:
            fieldnames = list(summary[0].keys())
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in summary:
                writer.writerow(row)
        print(f"[{timestamp()}] Summary written to {summary_path}")



def main():
    parser = argparse.ArgumentParser(
        description="Run sysbench and collect SHOW GLOBAL STATUS in parallel.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    g_conn = parser.add_argument_group("Connection options")
    g_shape = parser.add_argument_group("Data shape (prepare phase)")
    g_exec = parser.add_argument_group("Benchmark execution")
    g_mon = parser.add_argument_group("Monitoring")
    g_flow = parser.add_argument_group("Workflow control")
    g_workload = parser.add_argument_group("Workload shaping")
    g_out = parser.add_argument_group("Logging & output")
    g_conn.add_argument("--host", default=DEFAULTS["host"], help="MySQL host")
    g_conn.add_argument("--port", type=int, default=DEFAULTS["port"], help="MySQL port")
    g_conn.add_argument("--user", default=DEFAULTS["user"], help="MySQL user")
    g_conn.add_argument("--password", default=DEFAULTS["password"], help="MySQL password")
    g_conn.add_argument("--database", default=DEFAULTS["database"], help="Database name")
    g_shape.add_argument("--tables", type=int, default=DEFAULTS["tables"], help="Number of sbtest tables")
    mex = g_shape.add_mutually_exclusive_group()
    mex.add_argument("--table-size", type=int, default=DEFAULTS["table_size"], help="Rows per table during prepare. Mutually exclusive with --target-gb.")
    mex.add_argument("--target-gb", type=float, default=None, help="Quick estimator: target total dataset size in GiB; computes rows/table from expected bytes/row. Mutually exclusive with --table-size.")
    g_exec.add_argument("--threads", type=int, default=DEFAULTS["threads"], help="Sysbench worker threads during RUN phase")
    g_exec.add_argument("--duration", type=int, default=DEFAULTS["duration"], help="Run time in seconds")
    g_exec.add_argument("--report-interval", type=int, default=DEFAULTS["report_interval"], help="Seconds between sysbench reports")
    g_mon.add_argument("--status-interval", type=int, default=DEFAULTS["status_interval"], help="Seconds between SHOW GLOBAL STATUS samples")
    g_mon.add_argument("--metrics-interval", type=int, default=DEFAULTS.get("metrics_interval", 10), help="Seconds between INNODB_METRICS and SHOW ENGINE INNODB STATUS samples")
    g_mon.add_argument("--disable-ibuf-sampler", action="store_true", help="Disable change buffer (ibuf) samplers")
    g_flow.add_argument("--phase", choices=["init","run","both"], default="run", help="init: data load only; run: benchmark only; both: prepare then run")
    g_flow.add_argument("--skip-prepare", action="store_true", help="Skip the initial sysbench prepare (data load)")
    g_flow.add_argument("--reset-db", action="store_true", help="Drop and re-create the database before prepare (requires privileges)")
    g_workload.add_argument("--sysbench-script", default="oltp_write_only", help="Sysbench script (e.g., oltp_write_only, oltp_update_index)")
    g_workload.add_argument("--sb-extra", default="", help="Extra sysbench script options appended to both PREPARE and RUN")
    g_workload.add_argument("--create-extra-indexes", action="store_true", help="After PREPARE, add 3 non-unique secondary indexes per sbtestN: idx_c(c(20)), idx_pad(pad(20)), idx_kid(k,id)")
    g_out.add_argument("--out-dir", default=DEFAULTS["out_dir"], help="Base directory for logs; a per-test subfolder is created")
    g_out.add_argument("--test-name", default=None, help="Name for the per-test subfolder (skips interactive prompt)")
    g_out.add_argument("--scenario-file", default=None, help="JSON file describing scenarios to run sequentially or name of a built-in set (e.g., 'default_ibuf_3')")
    g_out.add_argument("--my-cnf", default="/etc/my.cnf", help="Path to my.cnf for scenario param updates")
    args = parser.parse_args()
    if args.scenario_file:
        run_scenarios(args)
    else:
        run_single(args)

if __name__ == "__main__":
    main()
