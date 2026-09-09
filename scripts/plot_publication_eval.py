#!/usr/bin/env python3
"""
Create publication plots comparing fast-path and baseline runs.

The script reads the two log files configured in ``LOG_FILES`` and writes
time-series, utilisation, call-path, migration, and correctness figures.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict

import numpy as np

# Config

LOG_FILES = {"fast": "hotel_raw - opt.log", "base": "hotel_raw - preopt.log"}
LABEL     = {"fast": "Fast path", "base": "Baseline"}
COLOUR    = {"fast": "#2563EB", "base": "#DC2626"}

C_MIGR = "#6B7280"   # grey band  — migration
C_FWD  = "#F59E0B"   # amber band — graceful forwarding

SCENARIOS = [(1, "search"), (2, "profile")]   # migrating rank -> scenario name

# Exclude the fixed-4096 B sweep app.
SWEEP_LO, SWEEP_HI = 2000, 16000
STREAM_DATA_MIN = 64          # bidi control frames are smaller than this

# Accept both archived and current bidi method names.
STREAM_SEND_METHODS = ("bidi_send", "__bidi_send__")

# Regex. Accept both rank and serviceId field names.

RE_RTT = re.compile(
    r"\[GRPC RTT\] app=(?P<app>\d+) source(?:ServiceId|Rank)=(?P<src>\d+)"
    r" dest(?:ServiceId|Rank)=(?P<dst>\d+)"
    r" callId=(?P<cid>\d+) kind=(?P<kind>\w+) method=(?P<method>\S+)"
    r" rtt_ns=(?P<rtt>\d+) send_ts_ns=(?P<send>\d+) retries=(?P<retries>\d+)"
    r" seqNum=(?P<seq>\d+) path=(?P<path>\w+)"
    r".*? req_bytes=(?P<req>\d+)"
)

RE_TRANSFER = re.compile(
    r"\[GRPC MIGRATE\] TRANSFER: app (?P<app>\d+) (?:serviceId|rank) (?P<rank>\d+)"
    r"(?:.*?\boutbox=(?P<outbox>\d+))?(?:.*?\bepoch=(?P<epoch>\d+))?"
)

RE_FWD = re.compile(
    r"\[GRPC FORWARD\] \[(?P<kind>STREAM|UNARY)\] callId (?P<cid>\d+)"
    r" streamId=(?P<sid>\d+) seqNum=(?P<seq>\d+)"
)

RE_FWD_INSTALL = re.compile(
    r"\[GRPC FORWARD\] Installed forward for app (?P<app>\d+)"
    r" (?:serviceId|rank) (?P<rank>\d+)"
    r".*?grace_ms=(?P<grace>\d+)"
)

# Prepare and transfer metrics are emitted by the source host.
RE_PREPARE_MS = re.compile(
    r"\[GRPC METRIC\] PREPARE_MS=(?P<ms>\d+) app (?P<app>\d+)"
    r" (?:serviceId|rank) (?P<rank>\d+)"
)

RE_TRANSFER_MS = re.compile(
    r"\[GRPC METRIC\] TRANSFER_MS=(?P<ms>\d+) app (?P<app>\d+)"
    r" (?:serviceId|rank) (?P<rank>\d+)"
)

# Commit metrics are emitted by the destination host.
RE_COMMIT_MS = re.compile(
    r"\[GRPC METRIC\] COMMIT_SERVE_MS=(?P<serve>\d+)"
    r" COMMIT_TAIL_MS=(?P<tail>\d+) COMMIT_TOTAL_MS=(?P<total>\d+)"
    r" app (?P<app>\d+) (?:serviceId|rank) (?P<rank>\d+)"
)

RE_UTIL = re.compile(
    r"\[HOST UTIL\] ts_ns=(?P<ts>\d+) cpu_pct=(?P<cpu>[\d.]+)"
    r" mem_pct=(?P<mem>[\d.]+) rss_bytes=(?P<rss>-?\d+) cores=(?P<cores>[\d.]+)"
)

# Correctness evidence

# "[GRPC DEDUPE] [UNARY] Cache hit: app <id> sourceServiceId <s> callId <n> ..."
RE_DEDUPE_HIT = re.compile(
    r"\[GRPC DEDUPE\] \[\w+\] Cache hit: app (?P<app>\d+)"
)

# Forwarded OK calls are subtracted in fencing_counts to avoid double counting.
RE_STATUS_SUMMARY = re.compile(
    r"\[GRPC STATUS\] Summary app (?P<app>\d+) (?:serviceId|rank) (?P<rank>\d+)"
    r" inbound=\[(?P<inbound>[^\]]*)\]"
    r" forwarded=\[(?P<forwarded>[^\]]*)\]"
)

# Retransmit metrics record replayed messages after migration.
RE_RETRANSMIT = re.compile(
    r"\[GRPC RETRANSMIT\] app (?P<app>\d+) stream (?P<sid>\d+)"
    r" requester (?P<requester>\d+) range \[(?P<from>\d+),[^)]*\)"
    r" candidates=(?P<candidates>\d+) replayed=(?P<replayed>\d+)"
    r" failed=(?P<failed>\d+)"
)

# Parsing

def parse_log(path: str) -> dict:
    """Parse RTT, migration, forwarding, utilisation, and correctness data."""
    rtt, transfers, fwd, fwd_installs, util = [], [], [], [], []
    dedupe_hits, status_summaries, retransmits = [], [], []
    # Store migration phases by app and rank.
    phases = defaultdict(dict)
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if "[HOST UTIL]" in line:
                m = RE_UTIL.search(line)
                if m:
                    util.append({
                        "ts": int(m.group("ts")),
                        "cpu": float(m.group("cpu")),
                        "mem": float(m.group("mem")),
                        # Worker lines carry the container prefix.
                        "proc": ("worker"
                                 if line.startswith("dist-test-server")
                                 else "main"),
                    })
                continue
            if "[GRPC " not in line:
                continue
            if "[GRPC METRIC]" in line:
                for rex, field in ((RE_PREPARE_MS, "prepare"),
                                   (RE_TRANSFER_MS, "transfer")):
                    m = rex.search(line)
                    if m:
                        key = (int(m.group("app")), int(m.group("rank")))
                        phases[key][field] = int(m.group("ms"))
                        break
                else:
                    m = RE_COMMIT_MS.search(line)
                    if m:
                        key = (int(m.group("app")), int(m.group("rank")))
                        phases[key]["commit_serve"] = int(m.group("serve"))
                        phases[key]["commit_tail"] = int(m.group("tail"))
                        phases[key]["commit_total"] = int(m.group("total"))
                continue
            m = RE_RTT.search(line)
            if m:
                d = m.groupdict()
                for k in ("app", "src", "dst", "cid", "rtt", "send",
                          "retries", "seq", "req"):
                    d[k] = int(d[k])
                rtt.append(d)
                continue
            m = RE_TRANSFER.search(line)
            if m:
                transfers.append({
                    "app": int(m.group("app")),
                    "rank": int(m.group("rank")),
                    "outbox": (int(m.group("outbox"))
                               if m.group("outbox") is not None else None),
                    "epoch": (int(m.group("epoch"))
                              if m.group("epoch") is not None else None),
                })
                continue
            m = RE_FWD.search(line)
            if m:
                fwd.append({"kind": m.group("kind"), "cid": int(m.group("cid")),
                            "seq": int(m.group("seq"))})
                continue
            m = RE_FWD_INSTALL.search(line)
            if m:
                fwd_installs.append({"app": int(m.group("app")),
                                     "rank": int(m.group("rank")),
                                     "grace_ms": int(m.group("grace"))})
                continue
            m = RE_DEDUPE_HIT.search(line)
            if m:
                dedupe_hits.append({"app": int(m.group("app"))})
                continue
            m = RE_RETRANSMIT.search(line)
            if m:
                retransmits.append({
                    "app": int(m.group("app")),
                    "requester": int(m.group("requester")),
                    "from": int(m.group("from")),
                    "candidates": int(m.group("candidates")),
                    "replayed": int(m.group("replayed")),
                    "failed": int(m.group("failed")),
                })
                continue
            m = RE_STATUS_SUMMARY.search(line)
            if m:
                def _kv(blob: str) -> dict:
                    out = {}
                    if blob.strip() == "none":
                        return out
                    for tok in blob.split(","):
                        if "=" in tok:
                            k, v = tok.split("=", 1)
                            out[k.strip()] = int(v)
                    return out
                status_summaries.append({"app": int(m.group("app")),
                                         "rank": int(m.group("rank")),
                                         "inbound": _kv(m.group("inbound")),
                                         "forwarded": _kv(m.group("forwarded"))})

    by_app = defaultdict(list)
    for r in rtt:
        by_app[r["app"]].append(r["req"])
    sweep_apps = {a for a, v in by_app.items()
                  if SWEEP_LO <= float(np.median(v)) <= SWEEP_HI}

    rank_app = {}
    for t in transfers:
        rank_app.setdefault(t["rank"], t["app"])

    # Apps with a graceful-forwarding window.
    fwd_apps = {fi["app"] for fi in fwd_installs}

    return {"rtt": rtt, "fwd": fwd, "sweep_apps": sweep_apps,
            "rank_app": rank_app, "fwd_apps": fwd_apps,
            "fwd_installs": fwd_installs, "util": util,
            "transfers": transfers, "dedupe_hits": dedupe_hits,
            "status_summaries": status_summaries, "retransmits": retransmits,
            "phases": dict(phases)}


# Series extraction

def series(parsed: dict, scenario_rank: int, kind: str) -> list[dict]:
    """Return one scenario traffic series ordered by send time."""
    app = parsed["rank_app"].get(scenario_rank)
    if app is None:
        return []
    if kind == "unary":
        rows = [r for r in parsed["rtt"]
                if r["app"] == app and r["kind"] == "unary"
                and r["method"] == "nearby"]
    else:
        rows = [r for r in parsed["rtt"]
                if r["app"] == app and r["kind"] == "stream"
                and r["method"] in STREAM_SEND_METHODS and r["dst"] == 2
                and r["req"] >= STREAM_DATA_MIN]
    return sorted(rows, key=lambda r: r["send"])


def xvals(rows: list[dict], kind: str) -> np.ndarray:
    key = "cid" if kind == "unary" else "seq"
    return np.array([r[key] for r in rows], dtype=float)


def migration_send_ts(parsed: dict, scenario_rank: int):
    """Return the first migration-affected send timestamp."""
    app = parsed["rank_app"].get(scenario_rank)
    ts = [r["send"] for r in parsed["rtt"]
          if r["app"] == app and r["retries"] > 0]
    return min(ts) if ts else None


def bands(parsed: dict, scenario_rank: int, rows: list[dict], kind: str):
    """Return migration and forwarding bands in series-axis units."""
    x = xvals(rows, kind)
    if x.size == 0:
        return None, None
    xr = x.max() - x.min()
    w = max(0.012 * xr, 2.0)          # minimum visible band width

    # Map the migration time onto the series axis.
    t_migr = migration_send_ts(parsed, scenario_rank)
    if t_migr is None:
        return None, None
    sends = np.array([r["send"] for r in rows], dtype=float)
    i = int(np.searchsorted(sends, t_migr))
    i = min(i, len(rows) - 1)
    m0 = x[i]

    # Include forwarding rows only for apps that installed forwarding.
    app = parsed["rank_app"].get(scenario_rank)
    fseqs = []
    if app in parsed["fwd_apps"]:
        key = "cid" if kind == "unary" else "seq"
        fseqs = sorted(f[key] for f in parsed["fwd"]
                       if f["kind"] == ("UNARY" if kind == "unary" else "STREAM"))

    if fseqs:
        # Draw migration before forwarding.
        migr = (m0 - w, m0)
        fband = (m0, max(fseqs[-1], m0 + w))
    else:
        migr = (m0 - w / 2, m0 + w / 2)
        fband = None
    return migr, fband


# Stats helpers

def five_num(vals):
    """Return minimum, average, percentiles, maximum, and count."""
    a = np.asarray(vals, dtype=float)
    return dict(mn=a.min(), avg=float(a.mean()), p50=np.percentile(a, 50),
                p99=np.percentile(a, 99), mx=a.max(), n=len(a))


def bar_yerr(st, style="avg_minmax"):
    """Return bar height and lower/upper whisker sizes."""
    if style == "p50_p99":
        y = float(st["p50"])
        return y, 0.0, max(0.0, float(st["p99"]) - y)
    y = float(st["avg"])
    return y, y - float(st["mn"]), float(st["mx"]) - y


def draw_bar_whiskers(ax, xs, ys, lo_err, hi_err, upper_only=False):
    """Draw error-bar whiskers for a bar series."""
    _, caps, _ = ax.errorbar(
        xs, ys, yerr=[lo_err, hi_err], fmt="none",
        ecolor="black", elinewidth=0.9, capsize=3.5, zorder=3)
    if upper_only and caps:
        caps[0].set_visible(False)


def path_rtt_ms(parsed: dict, kind: str, path: str) -> list[float]:
    """Return successful RTTs for a call path."""
    out = []
    for r in parsed["rtt"]:
        if (r["kind"] == kind and r["path"] == path and r["retries"] == 0
                and r["app"] not in parsed["sweep_apps"]):
            if kind == "stream":
                if (r["method"] not in STREAM_SEND_METHODS
                        or r["req"] < STREAM_DATA_MIN):
                    continue
            elif r["method"] != "nearby":
                continue
            out.append(r["rtt"] / 1e6)
    return out


def path_rps(parsed: dict, kind: str, path: str) -> list[float]:
    """Return per-second call rates for a call path."""
    per_app = defaultdict(list)
    for r in parsed["rtt"]:
        if (r["kind"] == kind and r["path"] == path and r["retries"] == 0
                and r["app"] not in parsed["sweep_apps"]):
            if kind == "stream":
                if (r["method"] not in STREAM_SEND_METHODS
                        or r["req"] < STREAM_DATA_MIN):
                    continue
            elif r["method"] != "nearby":
                continue
            per_app[r["app"]].append(r["send"])
    rates = []
    for ts in per_app.values():
        ts.sort()
        t0 = ts[0]
        buckets = defaultdict(int)
        for t in ts:
            buckets[int((t - t0) / 1e9)] += 1
        mx = max(buckets)
        # Drop the final partial bucket.
        rates.extend(buckets.get(i, 0) for i in range(mx))
    return [float(v) for v in rates]


def migration_ms(parsed: dict, kind: str) -> list[float]:
    """Return RTTs for calls retried during migration."""
    rows = series(parsed, 1 if kind == "unary" else 2, kind)
    return [r["rtt"] / 1e6 for r in rows if r["retries"] > 0]


def migration_phase_ms(parsed: dict, kind: str) -> dict | None:
    """Return measured prepare, transfer, commit, and total times."""
    rank = 1 if kind == "unary" else 2
    app = parsed["rank_app"].get(rank)
    if app is None:
        return None
    ph = parsed["phases"].get((app, rank))
    if not ph or "commit_total" not in ph:
        return None
    prepare = float(ph.get("prepare", 0))
    transfer = float(ph.get("transfer", 0))
    commit = float(ph["commit_total"])
    return {"prepare": prepare, "transfer": transfer, "commit": commit,
            "total": prepare + transfer + commit}


def rolling_rps(rows: list[dict], kind: str, win: int):
    """Return rolling throughput for successful calls."""
    succ = [r for r in rows if r["retries"] == 0]
    n = len(succ)
    if n < win + 1:
        return np.array([]), np.array([])
    x = xvals(succ, kind)
    ts = np.array([r["send"] for r in succ], dtype=float)
    xs, ys = [], []
    for i in range(n - win):
        dt = (ts[i + win] - ts[i]) / 1e9
        if dt <= 0:
            continue
        xs.append(x[i + win // 2])
        ys.append(win / dt)
    return np.array(xs), np.array(ys)


# Plot style

def set_style(mpl):
    mpl.rcParams.update({
        "font.family": "serif",
        "font.size": 9,
        "axes.titlesize": 10,
        "axes.labelsize": 9.5,
        "xtick.labelsize": 8.5,
        "ytick.labelsize": 8.5,
        "legend.fontsize": 8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "grid.linewidth": 0.5,
        "axes.axisbelow": True,
        "figure.dpi": 120,
        "savefig.dpi": 300,
        "legend.frameon": False,
    })


def save(fig, out_dir: str, name: str):
    fig.savefig(os.path.join(out_dir, f"{name}.png"), bbox_inches="tight")
    print(f"  saved: {os.path.join(out_dir, name)}.png")


def add_bands(ax, migr, fwd):
    """Shade migration and graceful-forwarding intervals."""
    if fwd is not None:
        ax.axvspan(fwd[0], fwd[1], color=C_FWD, alpha=0.35, zorder=0)
    if migr is not None:
        ax.axvspan(migr[0], migr[1], color=C_MIGR, alpha=0.75, zorder=1)


def band_legend_handles(with_fwd: bool):
    from matplotlib.patches import Patch
    h = [Patch(facecolor=C_MIGR, alpha=0.75, label="migration")]
    if with_fwd:
        h.append(Patch(facecolor=C_FWD, alpha=0.35, label="graceful forwarding"))
    return h


# Time-series figures: fast path and baseline

def fig_timeseries(data, scenario_rank, scenario, kind, metric, out_dir, plt):
    xlabel = "Call ID" if kind == "unary" else "Stream ID"
    yname = "RTT" if metric == "rtt" else "Throughput"
    name = (f"fig_{'rtt' if metric == 'rtt' else 'tput'}"
            f"_vs_{'callid' if kind == 'unary' else 'streamid'}_{scenario}")

    share = metric == "rtt"   # log RTT shares an axis; throughput scales differ
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.5), sharey=share)

    for ax, cfg, tag in zip(axes, ("fast", "base"), ("a", "b")):
        parsed = data[cfg]
        rows = series(parsed, scenario_rank, kind)
        if not rows:
            continue
        migr, fwd = bands(parsed, scenario_rank, rows, kind)
        add_bands(ax, migr, fwd)

        if metric == "rtt":
            x = xvals(rows, kind)
            y = np.array([r["rtt"] / 1e6 for r in rows])
            ax.plot(x, y, lw=0.6, color=COLOUR[cfg], zorder=2)
            ax.set_yscale("log")
        else:
            win = 25 if kind == "unary" else 200
            x, y = rolling_rps(rows, kind, win)
            if x.size:
                ax.plot(x, y, lw=0.9, color=COLOUR[cfg], zorder=2)
                ax.set_ylim(0, y.max() * 1.3)

        ax.set_xlabel(xlabel)
        ax.set_title(f"({tag}) {LABEL[cfg]}", fontsize=9)
        ax.set_xlim(left=0)

        # Keep the band legend inside the plot.
        ax.legend(handles=band_legend_handles(fwd is not None),
                  loc="upper right", frameon=True, framealpha=0.85,
                  facecolor="white", edgecolor="none",
                  fontsize=7, borderpad=0.35, handlelength=1.2,
                  handletextpad=0.4, labelspacing=0.3)

    if metric == "rtt":
        lo, hi = axes[0].get_ylim()
        axes[0].set_ylim(lo, hi * 5)   # headroom for the in-body legend

    axes[0].set_ylabel("RTT (ms)" if metric == "rtt" else "Throughput (RPC/s)")
    fig.suptitle(f"{yname} vs {xlabel} ({scenario.capitalize()})",
                 y=1.04, fontsize=11)
    fig.tight_layout()
    save(fig, out_dir, name)
    plt.close(fig)


# Host utilisation figures: fast path and baseline

UTIL_PROCS = (("main", "Test Process"), ("worker", "Worker Process"))


def fig_util_vs_time(data, scenario_rank, scenario, proc, proc_lbl, metric,
                     out_dir, plt):
    """Plot CPU or memory utilisation for one process."""
    yname = "CPU" if metric == "cpu" else "Memory"
    name = f"fig_{metric}_util_vs_time_{scenario}_{proc}"

    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.5), sharey=True)
    drew, ymax = False, 0.0

    for ax, cfg, tag in zip(axes, ("fast", "base"), ("a", "b")):
        parsed = data[cfg]
        samp = sorted((u for u in parsed["util"] if u["proc"] == proc),
                      key=lambda u: u["ts"])
        if not samp:
            continue
        drew = True
        # Share the time origin across processes.
        t0 = min(u["ts"] for u in parsed["util"])

        xs = np.array([(u["ts"] - t0) / 1e9 for u in samp])
        ys = np.array([u[metric] for u in samp])
        ax.plot(xs, ys, lw=0.9, color=COLOUR[cfg], zorder=2)
        ymax = max(ymax, float(ys.max()))

        # Map migration and forwarding windows to elapsed time.
        has_fwd = False
        t_migr = migration_send_ts(parsed, scenario_rank)
        if t_migr is not None:
            m = (t_migr - t0) / 1e9
            app = parsed["rank_app"].get(scenario_rank)
            grace_ms = [fi["grace_ms"] for fi in parsed["fwd_installs"]
                        if fi["app"] == app]
            if grace_ms:
                ax.axvspan(m, m + max(grace_ms) / 1e3,
                           color=C_FWD, alpha=0.35, zorder=0)
                has_fwd = True
            w = max(0.006 * float(xs.max()), 0.15)
            ax.axvspan(m - w / 2, m + w / 2,
                       color=C_MIGR, alpha=0.75, zorder=1)

        ax.set_xlabel("Time [s]")
        ax.set_title(f"({tag}) {LABEL[cfg]}", fontsize=9)
        ax.set_xlim(left=0)
        ax.legend(handles=band_legend_handles(has_fwd),
                  loc="upper right", frameon=True, framealpha=0.85,
                  facecolor="white", edgecolor="none",
                  fontsize=7, borderpad=0.35, handlelength=1.2,
                  handletextpad=0.4, labelspacing=0.3)

    if not drew:
        plt.close(fig)
        print(f"  skipped {name}: no [HOST UTIL] samples in the logs "
              "(re-run with the host-util sampler enabled)")
        return

    axes[0].set_ylim(0, max(ymax * 1.4, 1.0))  # headroom for in-body legend
    axes[0].set_ylabel(f"{yname} Usage [%]")
    fig.suptitle(f"{yname} Usage vs Time ({scenario.capitalize()}, {proc_lbl})",
                 y=1.04, fontsize=11)
    fig.tight_layout()
    save(fig, out_dir, name)
    plt.close(fig)


# Bar figures

def bar_panel(ax, stats_by_group, value_fmt=None, style="avg_minmax"):
    """Draw grouped bars and return their legend handles."""
    groups = list(stats_by_group.keys())
    x = np.arange(len(groups))
    width = 0.32
    handles = []
    upper_only = style == "p50_p99"

    for j, cfg in enumerate(("base", "fast")):
        xs, ys, lo_err, hi_err = [], [], [], []
        for gi, g in enumerate(groups):
            st = stats_by_group[g].get(cfg)
            if st is None:
                continue
            y, lo, hi = bar_yerr(st, style)
            xs.append(x[gi] + (j - 0.5) * width)
            ys.append(y)
            lo_err.append(lo)
            hi_err.append(hi)
        if not xs:
            continue
        bars = ax.bar(xs, ys, width * 0.92, color=COLOUR[cfg],
                      label=LABEL[cfg], zorder=2)
        handles.append(bars)
        draw_bar_whiskers(ax, xs, ys, lo_err, hi_err, upper_only=upper_only)
        if value_fmt is not None:
            # Place labels above the whisker cap.
            for xi, y, hi in zip(xs, ys, hi_err):
                ax.annotate(value_fmt.format(y), xy=(xi, y + hi),
                            xytext=(0, 3), textcoords="offset points",
                            ha="center", va="bottom", fontsize=6.5)

    ax.set_xticks(x)
    ax.set_xticklabels([g.capitalize() for g in groups])
    ax.set_xlabel("Call path")
    return handles


def fig_bars(stats_by_group, title, ylabel, name, out_dir, plt, log=False,
             value_fmt=None, style="avg_minmax"):
    fig, ax = plt.subplots(figsize=(3.4, 2.8))
    handles = bar_panel(ax, stats_by_group, value_fmt=value_fmt, style=style)
    ax.set_ylabel(ylabel)
    if log:
        ax.set_yscale("log")
    else:
        # Leave room for value labels.
        ax.set_ylim(bottom=0, top=ax.get_ylim()[1] * (1.08 if value_fmt else 1))
    ax.set_title(title, pad=24)
    # Place the legend above the plot body.
    ax.legend(handles=handles, loc="lower left", bbox_to_anchor=(-0.02, 1.0),
              ncol=2, borderaxespad=0, columnspacing=0.9, handletextpad=0.4,
              handlelength=1.3)
    fig.tight_layout()
    save(fig, out_dir, name)
    plt.close(fig)


def fig_migration_time(phase_by_group, out_dir, plt):
    """Plot stacked prepare, transfer, and commit durations."""
    groups = list(phase_by_group.keys())
    x = np.arange(len(groups))
    width = 0.32
    # Hatches distinguish migration phases.
    slices = (("prepare", "Prepare", ""),
              ("transfer", "Transfer", "///"),
              ("commit", "Commit", "..."))

    fig, ax = plt.subplots(figsize=(3.4, 2.8))
    cfg_handles, phase_handles = [], []
    for j, cfg in enumerate(("base", "fast")):
        xs, present = [], []
        for gi, g in enumerate(groups):
            ph = phase_by_group[g].get(cfg)
            if ph is None:
                continue
            xs.append(x[gi] + (j - 0.5) * width)
            present.append(ph)
        if not xs:
            continue
        bottom = np.zeros(len(xs))
        for key, lbl, hatch in slices:
            vals = np.array([p[key] for p in present], dtype=float)
            bars = ax.bar(xs, vals, width * 0.92, bottom=bottom,
                          color=COLOUR[cfg], hatch=hatch, edgecolor="white",
                          linewidth=0.5, zorder=2)
            bottom += vals
            if j == 0:
                phase_handles.append((lbl, hatch))
        cfg_handles.append(
            ax.bar(xs[:1], [0], width * 0.92, color=COLOUR[cfg],
                   label=LABEL[cfg], zorder=0))
        for xi, tot in zip(xs, bottom):
            ax.annotate(f"{tot:.0f}", xy=(xi, tot), xytext=(0, 2),
                        textcoords="offset points", ha="center", va="bottom",
                        fontsize=6.5)

    ax.set_xticks(x)
    ax.set_xticklabels([g.capitalize() for g in groups])
    ax.set_xlabel("Call path")
    ax.set_ylabel("Total migration time (ms)")
    ax.set_ylim(bottom=0)
    ax.set_title("Total Migration Time vs Call Path", pad=24)

    import matplotlib.patches as mpatches
    handles = [h for h in cfg_handles]
    handles += [mpatches.Patch(facecolor="0.75", hatch=hatch,
                               edgecolor="white", label=lbl)
                for lbl, hatch in phase_handles]
    ax.legend(handles=handles, loc="lower left", bbox_to_anchor=(-0.02, 1.0),
              ncol=3, borderaxespad=0, columnspacing=0.7, handletextpad=0.4,
              handlelength=1.1, fontsize=6.5)
    fig.tight_layout()
    save(fig, out_dir, "fig_migration_time_vs_callpath")
    plt.close(fig)


# Correctness figures and scorecard

def seq_stats(rows: list[dict], key: str = "seq"):
    """Count gaps, duplicates, and reorderings in an ordered series."""
    gaps = dupes = reorders = 0
    seen = set()
    prev = None
    for r in rows:
        s = r[key]
        if s in seen:
            dupes += 1
        seen.add(s)
        if prev is not None:
            if s < prev:
                reorders += 1
            elif s > prev + 1:
                gaps += s - prev - 1
        prev = s
    return {"gaps": gaps, "dupes": dupes, "reorders": reorders, "n": len(rows)}


def scenario_apps(parsed: dict) -> set:
    """Return apps represented by migration scenarios."""
    return set(parsed["rank_app"].values())


def dedupe_counts(parsed: dict):
    """Return sent, executed, retried, and deduplicated call counts."""
    apps = scenario_apps(parsed)
    rows = [r for r in parsed["rtt"] if r["app"] in apps]
    executed = len(rows)
    retried = sum(r["retries"] for r in rows)
    sent = executed + retried
    suppressed = sum(1 for h in parsed["dedupe_hits"] if h["app"] in apps)
    return sent, executed, retried, suppressed


def fencing_counts(parsed: dict):
    """Return accepted and stale-rejected call counts."""
    apps = scenario_apps(parsed)
    ok = forwarded_ok = unavail = 0
    for s in parsed["status_summaries"]:
        if s["app"] in apps:
            ok += s["inbound"].get("OK", 0)
            unavail += s["inbound"].get("UNAVAILABLE", 0)
            forwarded_ok += s.get("forwarded", {}).get("OK", 0)
    return ok - forwarded_ok, unavail


def retransmit_counts(parsed: dict, app=None):
    """Return replay request and message counts."""
    apps = {app} if app is not None else scenario_apps(parsed)
    rows = [t for t in parsed["retransmits"] if t["app"] in apps]
    return len(rows), sum(t["replayed"] for t in rows)


def stream_stall(rows: list[dict]):
    """Return the stream stall interval, or the largest delivery gap."""
    if len(rows) < 2:
        return None
    done = [r["send"] + r["rtt"] for r in rows]
    idx = next((i for i, r in enumerate(rows) if i > 0 and r["retries"] > 0),
               None)
    if idx is None:
        idx = max(range(1, len(rows)), key=lambda i: rows[i]["send"] - done[i - 1])
    return done[idx - 1], done[idx], rows[idx]


def fig_seq_integrity(data, out_dir, plt):
    """Plot profile-stream sequence progress and its migration stall."""
    fig, axes = plt.subplots(2, 2, figsize=(7.4, 5.0))

    drew = False
    for col, (cfg, tag) in enumerate(zip(("fast", "base"), ("a", "b"))):
        ax, axz = axes[0][col], axes[1][col]
        parsed = data[cfg]
        rows = series(parsed, 2, "stream")
        if not rows:
            continue
        drew = True

        origin = rows[0]["send"]
        x = np.array([(r["send"] + r["rtt"] - origin) / 1e6 for r in rows])
        y = np.array([r["seq"] for r in rows], dtype=float)

        for a in (ax, axz):
            a.plot(x, y, lw=1.0, color=COLOUR[cfg], zorder=3)

        stall = stream_stall(rows)
        if stall is not None:
            t0, t1, srow = stall
            s0, s1 = (t0 - origin) / 1e6, (t1 - origin) / 1e6
            for a in (ax, axz):
                a.axvspan(s0, s1, color=C_MIGR, alpha=0.75, zorder=1)

            # Zoom in because the stall is small relative to the full run.
            pad = max(3.0, (s1 - s0) * 0.9)
            axz.plot(x, y, ls="none", marker="o", markersize=2.4,
                     color=COLOUR[cfg], zorder=4)
            axz.set_xlim(s0 - pad, s1 + pad)
            axz.set_ylim(srow["seq"] - 5, srow["seq"] + 5)
            axz.yaxis.set_major_locator(
                plt.MaxNLocator(integer=True, nbins=6))

        ax.set_title(f"({tag}) {LABEL[cfg]}", fontsize=9)
        ax.set_xlim(left=0)
        axz.set_title("zoom on the migration", fontsize=8, color="0.3")
        ax.legend(handles=band_legend_handles(False),
                  loc="lower right", frameon=True, framealpha=0.85,
                  facecolor="white", edgecolor="none",
                  fontsize=7, borderpad=0.35, handlelength=1.2,
                  handletextpad=0.4, labelspacing=0.3)

    if not drew:
        plt.close(fig)
        print("  skipped fig_seq_integrity_profile: no stream rows")
        return

    for row in axes:
        for a in row:
            a.set_ylabel("Delivered sequence number")
            a.set_xlabel("Time [ms]")
            a.tick_params(labelleft=True)
    fig.suptitle("Profile Stream Continuity Across Migration",
                 y=1.0, fontsize=11)
    fig.tight_layout()
    save(fig, out_dir, "fig_seq_integrity_profile")
    plt.close(fig)


def _label_bars(ax, bars, rot=0):
    """Annotate bars with their counts, including zero-height bars."""
    floor = ax.get_ylim()[0]
    for b in bars:
        h = b.get_height()
        ax.annotate(f"{int(h):,}",
                    xy=(b.get_x() + b.get_width() / 2, h if h > 0 else floor),
                    xytext=(0, 2), textcoords="offset points",
                    ha="center", va="bottom", fontsize=6.5, rotation=rot)


def fig_dedupe(data, out_dir, plt):
    """Plot wire sends, executions, retries, and suppressed duplicates."""
    cats = ("Calls sent", "Calls executed\n(unique)", "Retried sends",
            "Duplicates\nsuppressed")
    x = np.arange(len(cats))
    width = 0.32

    fig, ax = plt.subplots(figsize=(5.2, 2.8))
    series_bars = []
    for j, cfg in enumerate(("base", "fast")):
        sent, executed, retried, suppressed = dedupe_counts(data[cfg])
        assert sent == executed + retried
        vals = (sent, executed, retried, suppressed)
        series_bars.append(
            ax.bar(x + (j - 0.5) * width, vals, width * 0.92,
                   color=COLOUR[cfg], label=LABEL[cfg], zorder=2))

    ax.set_yscale("log")
    ax.set_ylim(bottom=0.6, top=ax.get_ylim()[1] * 8)  # room for count labels
    for bars in series_bars:
        _label_bars(ax, bars)

    ax.set_xticks(x)
    ax.set_xticklabels(cats, fontsize=7.5)
    ax.set_ylabel("Count")
    ax.set_title("Deduplication Effectiveness vs Call Volume", pad=24)
    ax.legend(loc="lower left", bbox_to_anchor=(-0.02, 1.0), ncol=2,
              borderaxespad=0, columnspacing=0.9, handletextpad=0.4,
              handlelength=1.3)
    fig.tight_layout()
    save(fig, out_dir, "fig_dedupe_effectiveness")
    plt.close(fig)


def fig_epoch_fencing(data, out_dir, plt):
    """Plot accepted and stale-rejected calls from status summaries."""
    cats = ("Valid calls\naccepted", "Stale calls\nrejected")
    x = np.arange(len(cats))
    width = 0.32

    fig, ax = plt.subplots(figsize=(3.4, 2.8))
    series_bars = []
    for j, cfg in enumerate(("base", "fast")):
        accepted, rejected = fencing_counts(data[cfg])
        _sent, executed, _retried, suppressed = dedupe_counts(data[cfg])
        assert accepted == executed + suppressed, (
            f"{cfg}: accepted {accepted} != executed {executed} "
            f"+ suppressed {suppressed}")
        series_bars.append(
            ax.bar(x + (j - 0.5) * width, [accepted, rejected], width * 0.92,
                   color=COLOUR[cfg], label=LABEL[cfg], zorder=2))

    ax.set_yscale("log")
    ax.set_ylim(bottom=0.6, top=ax.get_ylim()[1] * 8)
    for bars in series_bars:
        _label_bars(ax, bars)

    ax.set_xticks(x)
    ax.set_xticklabels(cats, fontsize=8)
    ax.set_ylabel("Count")
    ax.set_title("Epoch Fencing vs Inbound Calls", pad=24)
    ax.legend(loc="lower left", bbox_to_anchor=(-0.02, 1.0), ncol=2,
              borderaxespad=0, columnspacing=0.9, handletextpad=0.4,
              handlelength=1.3)
    fig.tight_layout()
    save(fig, out_dir, "fig_epoch_fencing")
    plt.close(fig)


def write_scorecard(data, out_dir):
    """Invariant counts per config, written as a plain-text table."""
    rows = []
    for cfg in ("base", "fast"):
        parsed = data[cfg]
        st_u = seq_stats(series(parsed, 1, "unary"), key="cid")
        st_s = seq_stats(series(parsed, 2, "stream"))
        sent, executed, retried_sends, suppressed = dedupe_counts(parsed)
        accepted, rejected = fencing_counts(parsed)
        apps = scenario_apps(parsed)
        outbox = sum(t["outbox"] or 0 for t in parsed["transfers"]
                     if t["app"] in apps)
        epochs = sorted({t["epoch"] for t in parsed["transfers"]
                         if t["app"] in apps and t["epoch"] is not None})
        scen_rows = [r for r in parsed["rtt"] if r["app"] in apps]
        retried_calls = sum(1 for r in scen_rows if r["retries"] > 0)
        retx_reqs, replayed = retransmit_counts(parsed)
            # Repeated identity implies duplicate execution.
        dup_exec = len(scen_rows) - len({(r["app"], r["cid"], r["seq"])
                                         for r in scen_rows})
        rows.append({
            "Stream sequence gaps":        st_s["gaps"],
            "Stream duplicates delivered": st_s["dupes"],
            "Stream reorderings":          st_s["reorders"],
            "Unary duplicates delivered":  st_u["dupes"],
            "Unary reorderings":           st_u["reorders"],
            "Calls sent":                  sent,
            "Calls executed (unique)":     executed,
            "Retried sends":               retried_sends,
            "Retried calls":               retried_calls,
            "Retransmit requests":         retx_reqs,
            "Stream messages replayed":    replayed,
            "Duplicate executions":        dup_exec,
            "Duplicates suppressed":       suppressed,
            "Outbox residue at transfer":  outbox,
            "Valid calls accepted":        accepted,
            "Stale calls rejected":        rejected,
            "Epoch transitions":           "0 -> " + ",".join(map(str, epochs))
                                            if epochs else "none",
        })

    keys = list(rows[0].keys())
    w = max(len(k) for k in keys) + 2
    lines = ["Correctness scorecard (migration-scenario apps)",
             "=" * (w + 26), f"{'Invariant':<{w}}{'Baseline':>12}{'Fast path':>14}"]
    for k in keys:
        lines.append(f"{k:<{w}}{str(rows[0][k]):>12}{str(rows[1][k]):>14}")
    lines.append("")
    lines.append(
        "Accounting: calls sent = executed + retried sends; "
        "valid accepted = executed + duplicates suppressed "
        "(forwarded OK subtracted so grace-period calls are not counted twice). "
        "All-zero gap/duplicate/reorder rows + outbox residue 0 mean the stream "
        "survived migration intact; duplicates suppressed == dedupe-cache hits "
        "proves at-most-once execution.")

    text = "\n".join(lines)
    path = os.path.join(out_dir, "correctness_scorecard.txt")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text + "\n")
    print(f"  saved: {path}")
    print("\n" + text)


# Main

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--log-dir", default="grpc_metrics_out/logs")
    ap.add_argument("--out-dir", default="grpc_metrics_out/publication")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    data = {}
    for cfg, fname in LOG_FILES.items():
        path = os.path.join(args.log_dir, fname)
        if not os.path.exists(path):
            sys.exit(f"missing log: {path}")
        data[cfg] = parse_log(path)
        print(f"{fname}: {len(data[cfg]['rtt'])} RTT rows, "
              f"{len(data[cfg]['fwd'])} FORWARD rows, "
              f"{len(data[cfg]['util'])} HOST UTIL samples, "
              f"migrating apps {data[cfg]['rank_app']}")

    try:
        import matplotlib as mpl
        mpl.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("pip install matplotlib")
    set_style(mpl)

    print("Time-series figures…")
    for rank, scenario in SCENARIOS:
        for kind in ("unary", "stream"):
            for metric in ("rtt", "tput"):
                fig_timeseries(data, rank, scenario, kind, metric,
                               args.out_dir, plt)

    print("Host utilisation figures…")
    for rank, scenario in SCENARIOS:
        for proc, proc_lbl in UTIL_PROCS:
            for metric in ("cpu", "mem"):
                fig_util_vs_time(data, rank, scenario, proc, proc_lbl, metric,
                                 args.out_dir, plt)

    print("Bar figures…")
    # Baseline uses remote gRPC; fast path uses in-process calls.
    src_path = {"base": "grpc", "fast": "fastpath"}

    rtt_stats = {k: {} for k in ("unary", "stream")}
    rps_stats = {k: {} for k in ("unary", "stream")}
    mig_stats = {k: {} for k in ("unary", "stream")}
    phase_stats = {k: {} for k in ("unary", "stream")}
    for kind in ("unary", "stream"):
        for cfg in ("base", "fast"):
            vals = path_rtt_ms(data[cfg], kind, src_path[cfg])
            if vals:
                rtt_stats[kind][cfg] = five_num(vals)
            rates = path_rps(data[cfg], kind, src_path[cfg])
            if rates:
                rps_stats[kind][cfg] = five_num(rates)
            mig = migration_ms(data[cfg], kind)
            if mig:
                mig_stats[kind][cfg] = five_num(mig)
            ph = migration_phase_ms(data[cfg], kind)
            if ph:
                phase_stats[kind][cfg] = ph

    fig_bars(rtt_stats, "RTT vs Call Path", "RTT (ms)",
             "fig_rtt_vs_callpath", args.out_dir, plt, log=True,
             style="p50_p99")
    fig_bars(rps_stats, "Throughput vs Call Path", "Throughput (RPC/s)",
             "fig_tput_vs_callpath", args.out_dir, plt, log=True,
             style="avg_minmax")
    fig_bars(mig_stats, "Peak Migration Latency vs Call Path",
             "Peak migration latency (ms)",
             "fig_migration_vs_callpath", args.out_dir, plt,
             value_fmt="{:.0f}")
    fig_migration_time(phase_stats, args.out_dir, plt)

    print("Correctness figures…")
    fig_seq_integrity(data, args.out_dir, plt)
    fig_dedupe(data, args.out_dir, plt)
    fig_epoch_fencing(data, args.out_dir, plt)
    write_scorecard(data, args.out_dir)

    # Console tables
    def show(title, stats):
        print(f"\n{title}")
        for kind in ("unary", "stream"):
            for cfg in ("base", "fast"):
                st = stats[kind].get(cfg)
                if st:
                    print(f"  {kind:>6} {LABEL[cfg]:>9}: n={st['n']:>7} "
                          f"min={st['mn']:.3f} avg={st['avg']:.3f} "
                          f"p50={st['p50']:.3f} p99={st['p99']:.3f} "
                          f"max={st['mx']:.3f}")
    show("RTT (ms)", rtt_stats)
    show("Throughput (RPC/s)", rps_stats)
    show("Peak migration latency (ms)", mig_stats)

    print("\nTotal migration time (ms): Prepare + Transfer + Commit")
    for kind in ("unary", "stream"):
        for cfg in ("base", "fast"):
            ph = phase_stats[kind].get(cfg)
            if ph:
                print(f"  {kind:>6} {LABEL[cfg]:>9}: "
                      f"prepare={ph['prepare']:.0f} transfer={ph['transfer']:.0f} "
                      f"commit={ph['commit']:.0f} total={ph['total']:.0f}")

    for cfg in ("fast", "base"):
        util = data[cfg]["util"]
        if not util:
            continue
        print(f"\nHost utilisation ({LABEL[cfg]})")
        for proc, lbl in UTIL_PROCS:
            cpu = [u["cpu"] for u in util if u["proc"] == proc]
            mem = [u["mem"] for u in util if u["proc"] == proc]
            if cpu:
                print(f"  {lbl:>13}: cpu mean={np.mean(cpu):.1f}% "
                      f"max={max(cpu):.1f}%  mem mean={np.mean(mem):.2f}% "
                      f"max={max(mem):.2f}%  n={len(cpu)}")

    print("\nDone. Outputs in:", args.out_dir)


if __name__ == "__main__":
    main()
