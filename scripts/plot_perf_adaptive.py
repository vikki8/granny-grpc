#!/usr/bin/env python3
"""
Plot perf-adaptive latency, CPU, migration, and call-path metrics.
Migration bands and threshold lines are read from the log. Use ``--app`` to
select a run when the log contains more than one application.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict

import numpy as np

# Reuse parsing and styling from the publication plotter.
from plot_publication_eval import (
    COLOUR,
    C_FWD,
    C_MIGR,
    STREAM_DATA_MIN,
    STREAM_SEND_METHODS,
    band_legend_handles,
    bar_yerr,
    draw_bar_whiskers,
    five_num,
    parse_log,
    save,
    set_style,
)

# CPU line colour for the closed-loop figure.
C_CPU = "#059669"

# ServiceId-0 migration markers and their direction-matching statistics.
RE_TRIGGER = re.compile(
    r"\[PERF POLICY\] app (?P<app>\d+) MIGRATE_TRIGGERED"
    r" send_ts_ns=(?P<ts>\d+)"
    r" p50=(?P<p50>\d+)us p99=(?P<p99>\d+)us"
    r" rps=(?P<rps>\d+) cpu=(?P<cpu>[\d.]+)%"
)

# Planner-side migration direction.
RE_ACTION = re.compile(
    r"\[PERF POLICY\] app (?P<app>\d+) (?P<action>SCALE_IN|SCALE_OUT)"
    r".*?\(p50=(?P<p50>\d+)us p99=(?P<p99>\d+)us"
    r" rps=(?P<rps>\d+) cpu=(?P<cpu>[\d.]+)%\)"
)

# Scheduler thresholds used for reference lines.
RE_ACTIVE = re.compile(
    r"perf-adaptive scheduler active:.*?p50>(?P<p50>[\d.]+)us"
    r".*?scale-in when cpu<(?P<cpu>[\d.]+)%"
)

RE_UTIL = re.compile(r"\[HOST UTIL\] ts_ns=(?P<ts>\d+) cpu_pct=(?P<cpu>[\d.]+)"
                     r"(?:.*?cores=(?P<cores>[\d.]+))?")
RE_SEND_TS = re.compile(r"send_ts_ns=(?P<ts>\d+)")

# (kind, scenario) pairs used by the publication plotter.
PANELS = [("unary", "search"), ("stream", "profile")]

# A long send pause counts as migration disruption.
STALL_NS = 20e6          # 20 ms
CHAIN_SLACK_NS = 0.25e9  # stalls/retries within this of the band end extend it
SEARCH_NS = 2.5e9        # how far past the trigger disruption may reach


def log_src(line: str) -> str:
    """Return the emitting process from a log prefix."""
    head = line[:48]
    if "|" in head:
        return head.split("|", 1)[0].strip()
    return "main"


# Policy-marker parsing

def parse_triggers(path: str) -> dict[int, list[dict]]:
    """Return migration triggers grouped by app and sorted by timestamp."""
    out: dict[int, list[dict]] = defaultdict(list)
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if "MIGRATE_TRIGGERED" not in line:
                continue
            m = RE_TRIGGER.search(line)
            if m:
                out[int(m.group("app"))].append({
                    "ts": int(m.group("ts")),
                    "stats": (m.group("p50"), m.group("p99"),
                              m.group("rps"), m.group("cpu")),
                })
    return {a: sorted(v, key=lambda t: t["ts"]) for a, v in out.items()}


def parse_actions(path: str) -> dict[int, list[dict]]:
    """Return planner actions grouped by app."""
    out: dict[int, list[dict]] = defaultdict(list)
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if "[PERF POLICY]" not in line:
                continue
            m = RE_ACTION.search(line)
            if m:
                out[int(m.group("app"))].append({
                    "action": ("scale-in" if m.group("action") == "SCALE_IN"
                               else "scale-out"),
                    "stats": (m.group("p50"), m.group("p99"),
                              m.group("rps"), m.group("cpu")),
                })
    return out


def pair_directions(trig_list: list[dict], actions: list[dict]) -> list[str]:
    """Match migration triggers to planner directions."""
    labels = []
    unused = list(actions)
    for i, trig in enumerate(trig_list):
        hit = next((a for a in unused if a["stats"] == trig["stats"]), None)
        if hit is None and i < len(actions):
            hit = actions[i]
        if hit in unused:
            unused.remove(hit)
        labels.append(hit["action"] if hit else "auto-migrate")
    return labels


def parse_thresholds(path: str) -> dict:
    """Return scheduler thresholds from the log, if present."""
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if "perf-adaptive scheduler active" not in line:
                continue
            m = RE_ACTIVE.search(line)
            if m:
                return {"p50_hi_us": float(m.group("p50")),
                        "cpu_pct": float(m.group("cpu"))}
    return {}


# ServiceId-0 host tracking and CPU stitching

def parse_util_by_src(path: str) -> dict[str, list[dict]]:
    """Return host-util samples grouped by emitting process."""
    out: dict[str, list[dict]] = defaultdict(list)
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if "[HOST UTIL]" not in line:
                continue
            m = RE_UTIL.search(line)
            if m:
                cores = m.group("cores")
                out[log_src(line)].append({
                    "ts": int(m.group("ts")),
                    "cpu": float(m.group("cpu")),
                    "cores": float(cores) if cores else None,
                })
    return {s: sorted(v, key=lambda u: u["ts"]) for s, v in out.items()}


def cores_used(util_by_src: dict, srcs: list[str]) -> set:
    """Return distinct CPU denominators used by the selected hosts."""
    return {u["cores"] for s in srcs for u in util_by_src.get(s, [])
            if u["cores"] is not None}


def service0_src_segments(path: str, app: int) -> list[tuple[int, str]]:
    """Return the time segments for each process hosting serviceId 0."""
    tags = (f"app={app} sourceServiceId=0", f"app={app} sourceRank=0")
    pts: list[tuple[int, str]] = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if "[GRPC RTT]" not in line or not any(t in line for t in tags):
                continue
            m = RE_SEND_TS.search(line)
            if m:
                pts.append((int(m.group("ts")), log_src(line)))
    pts.sort()
    segs: list[tuple[int, str]] = []
    for ts, src in pts:
        if not segs or segs[-1][1] != src:
            segs.append((ts, src))
    return segs


def service0_cpu_series(util_by_src: dict, segs: list[tuple[int, str]]):
    """Stitch CPU samples across the processes hosting serviceId 0."""
    if not segs:
        return np.array([]), np.array([]), []
    xs, ys, used = [], [], []
    for i, (t0, src) in enumerate(segs):
        t1 = segs[i + 1][0] if i + 1 < len(segs) else float("inf")
        lo = -float("inf") if i == 0 else t0     # lead-in before first call
        samples = [u for u in util_by_src.get(src, []) if lo <= u["ts"] < t1]
        if samples and src not in used:
            used.append(src)
        xs.extend(u["ts"] for u in samples)
        ys.extend(u["cpu"] for u in samples)
    return np.array(xs, dtype=float), np.array(ys, dtype=float), used


# Series extraction

def app_series(parsed: dict, app: int, kind: str) -> list[dict]:
    """Return one traffic series for an app, ordered by send time."""
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


def app_all_rtt(parsed: dict, app: int) -> list[dict]:
    """Return all client-observed RTTs for an app."""
    rows = app_series(parsed, app, "unary") + app_series(parsed, app, "stream")
    return sorted(rows, key=lambda r: r["send"])


def rolling_p50_ms(rows: list[dict], win: int):
    """Return rolling p50 RTT values in milliseconds."""
    succ = [r for r in rows if r["retries"] == 0]
    n = len(succ)
    if n < win:
        return np.array([]), np.array([])
    sends = np.array([r["send"] for r in succ], dtype=float)
    rtts = np.array([r["rtt"] / 1e6 for r in succ], dtype=float)
    xs, ys = [], []
    for i in range(n - win + 1):
        xs.append(sends[i + win // 2])
        ys.append(float(np.median(rtts[i:i + win])))
    return np.array(xs), np.array(ys)


# Measured migration windows

def migration_windows(rows: list[dict], trig_list: list[dict]):
    """Return measured disruption windows for each migration trigger."""
    sends = np.array(sorted(r["send"] for r in rows), dtype=float)
    gaps = [(float(sends[i]), float(sends[i + 1]))
            for i in range(len(sends) - 1)
            if sends[i + 1] - sends[i] > STALL_NS]
    retries = sorted((r["send"], r["send"] + r["rtt"])
                     for r in rows if r["retries"] > 0)

    windows = []
    for trig in trig_list:
        t0 = float(trig["ts"])
        t1 = t0
        events = ([(a, b) for a, b in gaps
                   if t0 - CHAIN_SLACK_NS <= a <= t0 + SEARCH_NS] +
                  [(a, b) for a, b in retries
                   if t0 - CHAIN_SLACK_NS <= a <= t0 + SEARCH_NS])
        for a, b in sorted(events):
            if a <= t1 + CHAIN_SLACK_NS:
                t1 = max(t1, b)
        windows.append((t0, t1))
    return windows


def window_to_x(rows: list[dict], t0: float, t1: float):
    """Map a time window to send-order indices."""
    if not rows:
        return None
    sends = np.array([r["send"] for r in rows], dtype=float)
    i0 = min(int(np.searchsorted(sends, t0)), len(rows) - 1)
    i1 = min(int(np.searchsorted(sends, t1)), len(rows) - 1)
    return float(i0), float(i1)


def rolling_rps_idx(rows: list[dict], win: int):
    """Return rolling throughput on the send-order axis."""
    succ = [r for r in rows if r["retries"] == 0]
    n = len(succ)
    if n < win + 1:
        return np.array([]), np.array([])
    ts = np.array([r["send"] for r in succ], dtype=float)
    xs, ys = [], []
    for i in range(n - win):
        dt = (ts[i + win] - ts[i]) / 1e9
        if dt <= 0:
            continue
        xs.append(i + win // 2)
        ys.append(win / dt)
    return np.array(xs, dtype=float), np.array(ys, dtype=float)


# Closed-loop p50 RTT and CPU plot

def fig_closed_loop(parsed, app, trig_list, labels, thresholds, cpu_xs, cpu_ys,
                    out_dir, plt):
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    rows = app_all_rtt(parsed, app)
    if not rows:
        print("  skip fig_perf_closed_loop: no RTT rows for app", app)
        return

    windows = migration_windows(rows, trig_list)

    # Use one steady-clock origin for all series.
    origin = rows[0]["send"]
    if cpu_xs.size:
        origin = min(origin, float(cpu_xs.min()))
    if windows:
        origin = min(origin, min(t0 for t0, _ in windows))

    win = 40
    xs_lat, ys_lat = rolling_p50_ms(rows, win)
    if xs_lat.size == 0:
        print("  skip fig_perf_closed_loop: too few calls for a rolling p50")
        return
    tx_lat = (xs_lat - origin) / 1e9
    tmax = float(tx_lat.max()) if tx_lat.size else 1.0

    fig, ax = plt.subplots(figsize=(6.8, 3.3))

    # Shade measured migration windows and label their directions.
    min_w = 0.004 * tmax
    last_label_x = None
    label_y = 1.01
    for (t0, t1), lab in zip(windows, labels):
        b0 = (t0 - origin) / 1e9
        b1 = max((t1 - origin) / 1e9, b0 + min_w)
        ax.axvspan(b0, b1, color=C_MIGR, alpha=0.60, zorder=1)
        cx = (b0 + b1) / 2
        if last_label_x is not None and (cx - last_label_x) < 0.12 * tmax:
            label_y = 1.075 if label_y == 1.01 else 1.01
        else:
            label_y = 1.01
        ax.annotate(lab, xy=(cx, label_y), xycoords=("data", "axes fraction"),
                    ha="center", va="bottom", fontsize=7.5, color="#111827")
        last_label_x = cx

    # Latency uses the left log-scaled axis.
    ax.plot(tx_lat, ys_lat, lw=1.3, color=COLOUR["fast"], zorder=3)
    ax.set_yscale("log")
    ax.set_xlabel("Time [s]")
    ax.set_ylabel("p50 RTT [ms]", color=COLOUR["fast"])
    ax.tick_params(axis="y", colors=COLOUR["fast"])
    ax.set_xlim(left=0, right=max(tmax, 1.0))

    handles = [Line2D([0], [0], color=COLOUR["fast"], lw=1.3, label="p50 RTT")]
    if thresholds.get("p50_hi_us"):
        thr = thresholds["p50_hi_us"] / 1000.0
        ax.axhline(thr, color=COLOUR["fast"], ls=":", lw=0.9, zorder=2)
        handles.append(Line2D([0], [0], color=COLOUR["fast"], ls=":", lw=0.9,
                              label=f"p50 threshold ({thr:.2f} ms)"))

    # CPU of the process hosting serviceId 0 uses the right axis.
    if cpu_xs.size:
        ax2 = ax.twinx()
        ax2.grid(False)
        xu = (cpu_xs - origin) / 1e9
        ax2.plot(xu, cpu_ys, lw=1.2, color=C_CPU, zorder=3)
        ax2.set_ylim(0, max(100.0, float(cpu_ys.max()) * 1.15))
        ax2.set_ylabel("CPU [%]", color=C_CPU)
        ax2.tick_params(axis="y", colors=C_CPU)
        handles.append(Line2D([0], [0], color=C_CPU, lw=1.2,
                              label="CPU (service-0 host)"))
        if thresholds.get("cpu_pct") is not None:
            ax2.axhline(thresholds["cpu_pct"], color=C_CPU, ls="--", lw=0.9)
            handles.append(Line2D([0], [0], color=C_CPU, ls="--", lw=0.9,
                                  label=f"CPU ({thresholds['cpu_pct']:.0f}%)"))

    handles.append(Patch(facecolor=C_MIGR, alpha=0.60, label="migration"))

    ax.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, -0.22),
              ncol=3, frameon=False, fontsize=7, columnspacing=1.0,
              handlelength=1.6, handletextpad=0.4)

    ax.set_title("Self-healing autoscaling: metric-triggered migration and "
                 "recovery", fontsize=10, pad=22)
    fig.tight_layout()
    save(fig, out_dir, "fig_perf_closed_loop")
    plt.close(fig)


# Supporting per-scenario time series

def fig_perf_ts(parsed, app, trig_list, labels, kind, scenario, metric,
                out_dir, plt):
    # Send order remains monotonic across migration epochs.
    xlabel = ("Search call # (send order)" if kind == "unary"
              else "Profile message # (send order)")
    yname = "RTT" if metric == "rtt" else "Throughput"
    name = (f"fig_perf_{'rtt' if metric == 'rtt' else 'tput'}"
            f"_vs_{'callid' if kind == 'unary' else 'streamid'}_{scenario}")

    rows = app_series(parsed, app, kind)
    if not rows:
        print(f"  skip {name}: no {kind} rows for app {app}")
        return

    windows = migration_windows(app_all_rtt(parsed, app), trig_list)

    fig, ax = plt.subplots(figsize=(4.2, 2.6))

    x = np.arange(len(rows), dtype=float)
    min_w = 0.006 * len(rows)

    # Shade messages handled by graceful forwarding.
    fwd_band = None
    if app in parsed["fwd_apps"]:
        key = "cid" if kind == "unary" else "seq"
        fids = {f[key] for f in parsed["fwd"]
                if f["kind"] == ("UNARY" if kind == "unary" else "STREAM")}
        idxs = [i for i, r in enumerate(rows) if r[key] in fids]
        if idxs:
            fwd_band = (float(min(idxs)),
                        max(float(max(idxs)), float(min(idxs)) + min_w))
    if fwd_band:
        ax.axvspan(fwd_band[0], fwd_band[1], color=C_FWD, alpha=0.35, zorder=0)

    # Shade and label each measured migration window.
    last_label_x = None
    label_y = 1.01
    for (t0, t1), lab in zip(windows, labels):
        span = window_to_x(rows, t0, t1)
        if span is None:
            continue
        b0, b1 = span
        b1 = max(b1, b0 + min_w)
        ax.axvspan(b0, b1, color=C_MIGR, alpha=0.75, zorder=1)
        cx = (b0 + b1) / 2
        if last_label_x is not None and (cx - last_label_x) < 0.2 * len(rows):
            label_y = 1.09 if label_y == 1.01 else 1.01
        else:
            label_y = 1.01
        ax.annotate(lab, xy=(cx, label_y), xycoords=("data", "axes fraction"),
                    ha="center", va="bottom", fontsize=6.5, color="#111827")
        last_label_x = cx

    if metric == "rtt":
        y = np.array([r["rtt"] / 1e6 for r in rows])
        ax.plot(x, y, lw=0.6, color=COLOUR["fast"], zorder=2)
        ax.set_yscale("log")
    else:
        wlen = 25 if kind == "unary" else 200
        xr2, yr2 = rolling_rps_idx(rows, wlen)
        if xr2.size:
            ax.plot(xr2, yr2, lw=0.9, color=COLOUR["fast"], zorder=2)
            ax.set_ylim(0, yr2.max() * 1.3)

    ax.set_xlabel(xlabel)
    ax.set_xlim(left=0)
    ax.set_ylabel("RTT (ms)" if metric == "rtt" else "Throughput (RPC/s)")

    handles = band_legend_handles(fwd_band is not None)
    ax.legend(handles=handles, loc="upper right", frameon=True,
              framealpha=0.85, facecolor="white", edgecolor="none",
              fontsize=7, borderpad=0.35, handlelength=1.2,
              handletextpad=0.4, labelspacing=0.3)

    if metric == "rtt":
        lo, hi = ax.get_ylim()
        ax.set_ylim(lo, hi * 5)        # headroom for the in-body legend

    ax.set_title(f"{yname} — {scenario.capitalize()} "
                 f"({'unary' if kind == 'unary' else 'stream'})",
                 fontsize=10, pad=18)
    fig.tight_layout()
    save(fig, out_dir, name)
    plt.close(fig)


# Call-path bar figures

PATHS = ("grpc", "fastpath")
PATH_LABEL = {"grpc": "Baseline", "fastpath": "Fast path"}
PATH_COLOUR = {"grpc": COLOUR["base"], "fastpath": COLOUR["fast"]}

# Rolling window for throughput bars.
TPUT_WIN = {"unary": 25, "stream": 200}


def path_rtt_ms(rows: list[dict], path: str) -> list[float]:
    """Successful-call RTTs (ms) on one call path."""
    return [r["rtt"] / 1e6 for r in rows
            if r["path"] == path and r["retries"] == 0]


def path_rps(rows: list[dict], path: str, win: int) -> list[float]:
    """Return rolling call rates without crossing path changes."""
    out: list[float] = []
    run: list[dict] = []
    for r in rows + [None]:
        if r is not None and r["path"] == path:
            run.append(r)
            continue
        if len(run) > win:
            _, ys = rolling_rps_idx(run, win)
            out.extend(float(v) for v in ys)
        run = []
    return out


def fig_perf_bars(stats_by_kind, ylabel, title, name, out_dir, plt, log=False,
                  style="avg_minmax"):
    """Plot call-path bars using the selected error-bar style."""
    from matplotlib.patches import Patch

    groups = [k for k in ("unary", "stream") if stats_by_kind.get(k)]
    if not groups:
        print(f"  skip {name}: no per-path samples")
        return

    label = {"unary": "Search", "stream": "Profile"}
    x = np.arange(len(groups))
    width = 0.32
    upper_only = style == "p50_p99"

    fig, ax = plt.subplots(figsize=(3.4, 2.8))
    handles = []
    for j, p in enumerate(PATHS):
        xs, ys, lo_err, hi_err = [], [], [], []
        for gi, g in enumerate(groups):
            st = stats_by_kind[g].get(p)
            if st is None:
                continue
            y, lo, hi = bar_yerr(st, style)
            xs.append(x[gi] + (j - 0.5) * width)
            ys.append(y)
            lo_err.append(lo)
            hi_err.append(hi)
        if not xs:
            continue
        ax.bar(xs, ys, width * 0.92, color=PATH_COLOUR[p], zorder=2)
        draw_bar_whiskers(ax, xs, ys, lo_err, hi_err, upper_only=upper_only)
        handles.append(Patch(facecolor=PATH_COLOUR[p], label=PATH_LABEL[p]))

    ax.set_xticks(x)
    ax.set_xticklabels([label[g] for g in groups])
    ax.set_xlabel("Granule")
    ax.set_ylabel(ylabel)
    if log:
        ax.set_yscale("log")
    ax.set_title(title, pad=24)
    ax.legend(handles=handles, loc="lower left", bbox_to_anchor=(-0.02, 1.0),
              ncol=2, borderaxespad=0, columnspacing=0.9, handletextpad=0.4,
              handlelength=1.3, fontsize=7)
    fig.tight_layout()
    save(fig, out_dir, name)
    plt.close(fig)


def path_stats(parsed, app):
    """Return RTT and throughput statistics grouped by kind and path."""
    rtt = {k: {} for k in ("unary", "stream")}
    rps = {k: {} for k in ("unary", "stream")}
    for kind in ("unary", "stream"):
        rows = app_series(parsed, app, kind)
        if not rows:
            continue
        for p in PATHS:
            vals = path_rtt_ms(rows, p)
            if vals:
                rtt[kind][p] = five_num(vals)
            rates = path_rps(rows, p, TPUT_WIN[kind])
            if rates:
                rps[kind][p] = five_num(rates)
    return rtt, rps


# Main

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--log", default="grpc_metrics_out/hotel_raw.log",
                    help="perf-adaptive run log (must contain MIGRATE_TRIGGERED)")
    ap.add_argument("--out-dir", default="grpc_metrics_out/perf_adaptive")
    ap.add_argument("--app", type=int, default=None,
                    help="specific perf-adaptive app id to plot")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        sys.exit(f"missing log: {args.log}")
    os.makedirs(args.out_dir, exist_ok=True)

    parsed = parse_log(args.log)
    triggers = parse_triggers(args.log)
    actions = parse_actions(args.log)
    thresholds = parse_thresholds(args.log)
    if not triggers:
        sys.exit("no [PERF POLICY] ... MIGRATE_TRIGGERED markers found — is "
                 "this a perf-adaptive run (BATCH_SCHEDULER_MODE=perf-adaptive)?")

    print(f"{os.path.basename(args.log)}: {len(parsed['rtt'])} RTT rows, "
          f"{len(parsed['util'])} HOST UTIL samples; "
          f"perf-adaptive apps {sorted(triggers)}")
    if thresholds:
        print(f"thresholds: p50_hi={thresholds['p50_hi_us']:.0f}us "
              f"cpu={thresholds['cpu_pct']:.0f}%")

    if args.app is not None:
        if args.app not in triggers:
            sys.exit(f"app {args.app} has no MIGRATE_TRIGGERED marker "
                     f"(available: {sorted(triggers)})")
        app = args.app
    else:
        # Prefer the app with the fullest timeline.
        app = max(triggers, key=lambda a: sum(1 for r in parsed["rtt"]
                                              if r["app"] == a))
    trig_list = triggers[app]
    labels = pair_directions(trig_list, actions.get(app, []))

    # Stitch CPU samples from each serviceId-0 host.
    util_by_src = parse_util_by_src(args.log)
    segs = service0_src_segments(args.log, app)
    cpu_xs, cpu_ys, cpu_srcs = service0_cpu_series(util_by_src, segs)

    windows = migration_windows(app_all_rtt(parsed, app), trig_list)
    origin = min(r["send"] for r in parsed["rtt"] if r["app"] == app)
    print(f"plotting perf-adaptive app {app}: {len(trig_list)} migration(s)")
    for (t0, t1), lab in zip(windows, labels):
        print(f"  {lab:>10}: t={{{(t0 - origin) / 1e9:.3f} .. "
              f"{(t1 - origin) / 1e9:.3f}}}s "
              f"(disruption {(t1 - t0) / 1e6:.0f} ms)")
    print(f"CPU series follows service-0 host: {' -> '.join(cpu_srcs) or 'n/a'}")
    cores = cores_used(util_by_src, cpu_srcs)
    if len(cores) > 1:
        print("WARNING: service-0's hosts report different CPU denominators "
              f"(cores={sorted(cores)}). The same load then reads as different "
              "CPU%, so steps in the CPU curve at a migration are an artefact — "
              "set PERF_CPU_CORES identically on every Faasm host.")

    try:
        import matplotlib as mpl
        mpl.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("pip install matplotlib")
    set_style(mpl)

    print("Closed-loop recovery figure…")
    fig_closed_loop(parsed, app, trig_list, labels, thresholds, cpu_xs, cpu_ys,
                    args.out_dir, plt)

    print("Per-scenario time-series figures…")
    for kind, scenario in PANELS:
        for metric in ("rtt", "tput"):
            fig_perf_ts(parsed, app, trig_list, labels, kind, scenario, metric,
                        args.out_dir, plt)

    print("Call-path bar figures…")
    rtt_stats, rps_stats = path_stats(parsed, app)
    fig_perf_bars(rtt_stats, "RTT (ms)", "RTT vs Call Path",
                  "fig_perf_rtt_vs_callpath", args.out_dir, plt, log=True,
                  style="p50_p99")
    fig_perf_bars(rps_stats, "Throughput (RPC/s)", "Throughput vs Call Path",
                  "fig_perf_tput_vs_callpath", args.out_dir, plt, log=True,
                  style="avg_minmax")

    for title, stats in (("RTT (ms)", rtt_stats),
                         ("Throughput (RPC/s)", rps_stats)):
        print(f"\n{title}")
        for kind in ("unary", "stream"):
            for p in PATHS:
                st = stats[kind].get(p)
                if st:
                    print(f"  {kind:>6} {PATH_LABEL[p]:>21}: n={st['n']:>6} "
                          f"min={st['mn']:.3f} avg={st['avg']:.3f} "
                          f"p50={st['p50']:.3f} p99={st['p99']:.3f} "
                          f"max={st['mx']:.3f}")

    print("\nDone. Outputs in:", args.out_dir)


if __name__ == "__main__":
    main()
