# -*- coding: utf-8 -*-
"""
安裝 tw-alpha-reporting（報表輸出 + 批量回測）到目前目錄：
- scripts/make_report.py
- scripts/batch_backtest.py
- reports/reporting.py
- templates/report_template.md.j2
- configs/batch_backtest.example.yaml
- README_REPORTING.md

用法：
  python install_tw_alpha_reporting.py            # 安裝到目前資料夾
  python install_tw_alpha_reporting.py --force    # 覆寫已存在的檔案
"""
from __future__ import annotations
import os, argparse, textwrap, json

FILES = {
  "scripts/make_report.py": r'''
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, argparse
from datetime import datetime
import pandas as pd
import numpy as np
from reports.reporting import (
    to_nav_from_returns, to_returns_from_close, drawdown,
    PerfStats, plot_nav_vs_bench, plot_drawdown, plot_rolling_sharpe,
    read_series, write_series_csv, try_jinja_render, save_stats_json,
)

def parse_args():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--returns-csv", type=str, help="策略報酬 CSV 路徑")
    g.add_argument("--nav-csv", type=str, help="策略 NAV CSV 路徑")
    p.add_argument("--date-col", type=str, default="date")
    p.add_argument("--value-col", type=str, default=None, help="策略數值欄位，returns 或 nav")
    p.add_argument("--benchmark-csv", type=str, default=None, help="Benchmark CSV（可省略）")
    p.add_argument("--bench-type", type=str, default="close", choices=["returns", "nav", "close"])
    p.add_argument("--bench-date-col", type=str, default="date")
    p.add_argument("--bench-value-col", type=str, default=None)
    p.add_argument("--out-dir", type=str, required=True)
    p.add_argument("--freq", type=str, default="D", help="D/W/M, 影響年化換算")
    p.add_argument("--rf", type=float, default=0.0, help="年化無風險利率（Sharpe 用）")
    p.add_argument("--title", type=str, default="回測報告")
    p.add_argument("--name", type=str, default=None, help="報表檔名前綴")
    return p.parse_args()

def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    prefix = (args.name or "report").replace(" ", "_")

    # 讀策略資料
    if args.returns_csv:
        s = read_series(args.returns_csv, args.date_col, args.value_col or "ret")
        nav = to_nav_from_returns(s, start_nav=1.0)
    else:
        nav = read_series(args.nav_csv, args.date_col, args.value_col or "nav").rename("nav")

    # Benchmark（可選）
    bench_nav, bench_stats = None, None
    if args.benchmark_csv:
        braw = read_series(args.benchmark_csv, args.bench_date_col, args.bench_value_col or ("ret" if args.bench_type=="returns" else "close"))
        if args.bench_type == "returns":
            bench_nav = to_nav_from_returns(braw, start_nav=1.0)
        elif args.bench_type == "nav":
            bench_nav = braw.rename("nav")
        else:
            bench_nav = to_nav_from_returns(to_returns_from_close(braw), start_nav=1.0)

    # 指標
    returns = nav.pct_change().fillna(0.0).rename("ret")
    stats = PerfStats.compute(returns, rf=args.rf, freq=args.freq)

    # 輸出資料
    write_series_csv(nav, os.path.join(args.out_dir, f"{prefix}_nav.csv"))
    write_series_csv(drawdown(nav), os.path.join(args.out_dir, f"{prefix}_drawdown.csv"))
    save_stats_json(stats, os.path.join(args.out_dir, f"{prefix}_metrics_strategy.json"))

    # Benchmark 指標與檔案
    if bench_nav is not None:
        write_series_csv(bench_nav, os.path.join(args.out_dir, f"{prefix}_bench_nav.csv"))
        bret = bench_nav.pct_change().fillna(0.0)
        bench_stats = PerfStats.compute(bret, rf=args.rf, freq=args.freq)
        save_stats_json(bench_stats, os.path.join(args.out_dir, f"{prefix}_metrics_benchmark.json"))

    # 圖表
    plot_drawdown(nav, os.path.join(args.out_dir, f"{prefix}_drawdown.png"))
    if bench_nav is not None:
        plot_nav_vs_bench(nav, bench_nav, os.path.join(args.out_dir, f"{prefix}_nav_vs_bench.png"), title=args.title)
    else:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10,5))
        (nav / nav.iloc[0]).plot(label="Strategy")
        plt.title(args.title); plt.xlabel("Date"); plt.ylabel("Indexed NAV")
        plt.legend(); plt.tight_layout()
        plt.savefig(os.path.join(args.out_dir, f"{prefix}_nav.png"), dpi=160)
        plt.close()

    plot_rolling_sharpe(returns, os.path.join(args.out_dir, f"{prefix}_rolling_sharpe.png"), window=63, freq=args.freq)

    # Markdown 報表
    out_md = os.path.join(args.out_dir, f"{prefix}_report.md")
    context = {
        "title": args.title,
        "strategy": stats.__dict__,
        "benchmark": bench_stats.__dict__ if bench_stats else None,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "images": {
            "drawdown": f"{prefix}_drawdown.png",
            "nav_or_vs": f"{prefix}_nav_vs_bench.png" if bench_nav is not None else f"{prefix}_nav.png",
            "rolling_sharpe": f"{prefix}_rolling_sharpe.png",
        }
    }
    template_path = os.path.join(os.path.dirname(__file__), "..", "templates", "report_template.md.j2")
    from reports.reporting import generate_markdown_report, try_jinja_render
    if os.path.exists(template_path) and try_jinja_render(context, template_path, out_md):
        pass
    else:
        generate_markdown_report(stats, bench_stats, out_md, extra={"產出時間": context["generated_at"]}, title=args.title)
    print(f"[OK] 報表已產出於: {args.out_dir}")

if __name__ == "__main__":
    main()
  ''',

  "reports/reporting.py": r'''
# -*- coding: utf-8 -*-
from __future__ import annotations
import math, json, typing as T
from dataclasses import dataclass, asdict
import numpy as np, pandas as pd, matplotlib.pyplot as plt

def _to_series(x, name="value") -> pd.Series:
    if isinstance(x, pd.Series):
        s = x.copy(); s.name = s.name or name; return s
    if isinstance(x, (list, tuple, np.ndarray)):
        return pd.Series(x, name=name)
    if isinstance(x, pd.DataFrame):
        if x.shape[1] != 1: raise ValueError("DataFrame 只能有單一欄位或請先選一欄")
        s = x.iloc[:,0]; s.name = s.name or name; return s
    raise TypeError("不支援的輸入型別")

def ensure_datetime_index(s: pd.Series) -> pd.Series:
    if isinstance(s, pd.DataFrame): raise TypeError("請傳入 Series")
    if s.index.dtype == "O": s.index = pd.to_datetime(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s

def to_nav_from_returns(returns: pd.Series, start_nav: float=1.0) -> pd.Series:
    r = ensure_datetime_index(_to_series(returns,"ret")).astype(float).fillna(0.0)
    nav = (1.0 + r).cumprod() * float(start_nav); nav.name = "nav"; return nav

def to_returns_from_close(close: pd.Series) -> pd.Series:
    c = ensure_datetime_index(_to_series(close,"close")).astype(float)
    ret = c.pct_change().fillna(0.0); ret.name = "ret"; return ret

def align(a: pd.Series, b: pd.Series) -> T.Tuple[pd.Series,pd.Series]:
    a = ensure_datetime_index(_to_series(a)); b = ensure_datetime_index(_to_series(b))
    idx = a.index.intersection(b.index); return a.reindex(idx), b.reindex(idx)

def annualize_factor(freq: str) -> int:
    f = (freq or "").upper()
    return 252 if f in ("","D","DAILY") else 52 if f in ("W","WEEKLY") else 12 if f in ("M","MONTHLY") else 1

@dataclass
class PerfStats:
    start_date: str; end_date: str; periods: int
    cagr: float; total_return: float; vol: float; sharpe: float; sortino: float
    max_dd: float; calmar: float; win_rate: float; best_day: float; worst_day: float

    @classmethod
    def compute(cls, returns: pd.Series, rf: float=0.0, freq: str="D") -> "PerfStats":
        r = ensure_datetime_index(_to_series(returns,"ret")).astype(float).replace([np.inf,-np.inf],np.nan).dropna()
        if len(r)==0: raise ValueError("回報序列為空")
        ann = annualize_factor(freq); nav = (1+r).cumprod()
        total_return = float(nav.iloc[-1]-1.0)
        years = max((r.index[-1]-r.index[0]).days/365.25,1e-12)
        cagr = (float(nav.iloc[-1])**(1.0/years))-1.0 if years>0 else np.nan
        vol = float(r.std(ddof=0))*math.sqrt(ann); er = float(r.mean())*ann - rf
        sharpe = er/vol if vol>1e-12 else np.nan
        downside = r[r<0]; dvol = float(downside.std(ddof=0))*math.sqrt(ann) if len(downside) else np.nan
        sortino = er/dvol if dvol and dvol>1e-12 else np.nan
        dd = drawdown(nav); max_dd = float(dd.min()); calmar = cagr/abs(max_dd) if max_dd<-1e-12 else np.nan
        win_rate = float((r>0).mean()); best_day = float(r.max()); worst_day = float(r.min())
        return cls(r.index[0].strftime("%Y-%m-%d"), r.index[-1].strftime("%Y-%m-%d"), len(r),
                   cagr,total_return,vol,sharpe,sortino,max_dd,calmar,win_rate,best_day,worst_day)

def drawdown(nav: pd.Series) -> pd.Series:
    n = ensure_datetime_index(_to_series(nav,"nav")).astype(float)
    dd = (n/n.cummax())-1.0; dd.name="drawdown"; return dd

def plot_nav_vs_bench(nav: pd.Series, bench_nav: pd.Series, out_png: str, title="NAV vs Benchmark"):
    n,b = align(_to_series(nav,"nav"), _to_series(bench_nav,"bench_nav"))
    plt.figure(figsize=(10,5)); (n/n.iloc[0]).plot(label="Strategy"); (b/b.iloc[0]).plot(label="Benchmark")
    plt.legend(); plt.title(title); plt.xlabel("Date"); plt.ylabel("Indexed NAV"); plt.tight_layout()
    plt.savefig(out_png,dpi=160); plt.close()

def plot_drawdown(nav: pd.Series, out_png: str, title="Drawdown"):
    dd = drawdown(nav); plt.figure(figsize=(10,3.5)); dd.plot()
    plt.title(title); plt.xlabel("Date"); plt.ylabel("Drawdown"); plt.tight_layout()
    plt.savefig(out_png,dpi=160); plt.close()

def rolling_sharpe(returns: pd.Series, window: int, ann_factor: int) -> pd.Series:
    r = ensure_datetime_index(_to_series(returns,"ret")).astype(float)
    rs = r.rolling(window).mean()/r.rolling(window).std(ddof=0)
    rs = rs*math.sqrt(ann_factor); rs.name="rolling_sharpe"; return rs

def plot_rolling_sharpe(returns: pd.Series, out_png: str, window: int=63, freq: str="D"):
    rs = rolling_sharpe(returns, window, ann_factor=annualize_factor(freq))
    plt.figure(figsize=(10,3.5)); rs.plot()
    plt.title(f"Rolling Sharpe ({window})"); plt.xlabel("Date"); plt.ylabel("Sharpe"); plt.tight_layout()
    plt.savefig(out_png,dpi=160); plt.close()

def read_series(path: str, date_col: str, value_col: str) -> pd.Series:
    df = pd.read_csv(path); s = pd.Series(df[value_col].values, index=pd.to_datetime(df[date_col].values), name=value_col)
    return ensure_datetime_index(s)

def write_series_csv(s: pd.Series, path: str, date_col="date", value_col: str|None=None):
    s = ensure_datetime_index(_to_series(s)); vc = value_col or s.name or "value"
    pd.DataFrame({date_col: s.index, vc: s.values}).to_csv(path, index=False)

def generate_markdown_report(strategy_stats: PerfStats, bench_stats: PerfStats|None, out_md: str, extra: dict|None=None, title="回測報告"):
    def pct(x): return "—" if (x is None) or (isinstance(x,float) and (np.isnan(x) or np.isinf(x))) else f"{x*100:.2f}%"
    lines = [
      f"# {title}", "",
      f"- 區間：{strategy_stats.start_date} → {strategy_stats.end_date}（{strategy_stats.periods} 筆）", "",
      "## 指標（策略）",
      f"- CAGR：{pct(strategy_stats.cagr)}",
      f"- 總報酬：{pct(strategy_stats.total_return)}",
      f"- 波動（年化）：{pct(strategy_stats.vol)}",
      f"- Sharpe：{strategy_stats.sharpe:.2f}" if strategy_stats.sharpe==strategy_stats.sharpe else "- Sharpe：—",
      f"- Sortino：{strategy_stats.sortino:.2f}" if strategy_stats.sortino==strategy_stats.sortino else "- Sortino：—",
      f"- 最大回撤：{pct(strategy_stats.max_dd)}",
      f"- Calmar：{strategy_stats.calmar:.2f}" if strategy_stats.calmar==strategy_stats.calmar else "- Calmar：—",
      f"- 勝率：{pct(strategy_stats.win_rate)}",
      f"- 單日最好 / 最差：{pct(strategy_stats.best_day)} / {pct(strategy_stats.worst_day)}",
    ]
    if bench_stats:
        lines += ["","## 指標（Benchmark）",
                  f"- CAGR：{pct(bench_stats.cagr)}",
                  f"- 總報酬：{pct(bench_stats.total_return)}",
                  f"- 波動（年化）：{pct(bench_stats.vol)}",
                  f"- Sharpe：{bench_stats.sharpe:.2f}" if bench_stats.sharpe==bench_stats.sharpe else "- Sharpe：—",
                  f"- 最大回撤：{pct(bench_stats.max_dd)}"]
    if extra:
        lines += ["","## 其他"] + [f"- {k}: {v}" for k,v in extra.items()]
    with open(out_md,"w",encoding="utf-8") as f: f.write("\n".join(lines))

def try_jinja_render(context: dict, template_path: str, out_md: str) -> bool:
    try:
        from jinja2 import Template
    except Exception:
        return False
    with open(template_path,"r",encoding="utf-8") as f: tpl = Template(f.read())
    with open(out_md,"w",encoding="utf-8") as f: f.write(tpl.render(**context))
    return True

def save_stats_json(stats: PerfStats, path: str):
    with open(path,"w",encoding="utf-8") as f: json.dump(asdict(stats), f, ensure_ascii=False, indent=2)
  ''',

  "scripts/batch_backtest.py": r'''
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, argparse, yaml, itertools, subprocess, json
from datetime import datetime
from typing import List
import pandas as pd, numpy as np

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--factors-path", type=str, required=True)
    p.add_argument("--base-config", type=str, required=True)
    p.add_argument("--combos", type=str, required=True)
    p.add_argument("--out-root", type=str, required=True)
    p.add_argument("--jobs", type=int, default=1)
    p.add_argument("--engine", type=str, choices=["script","builtin"], default="script")
    p.add_argument("--make-report", action="store_true")
    p.add_argument("--freq", type=str, default="D")
    p.add_argument("--chunk-months", type=int, default=0)
    return p.parse_args()

def load_yaml(path:str)->dict:
    with open(path,"r",encoding="utf-8") as f: return yaml.safe_load(f)
def save_yaml(obj:dict, path:str):
    with open(path,"w",encoding="utf-8") as f: yaml.safe_dump(obj,f,allow_unicode=True,sort_keys=False)
def timestamp()->str: return datetime.now().strftime("%Y%m%d-%H%M%S")

def zscore(x: pd.Series)->pd.Series: return (x-x.mean())/(x.std(ddof=0)+1e-12)
def build_combo_name(cols: List[str], weights: List[float]|None=None, method: str="zscore_mean")->str:
    base = "score__" + "_".join(cols)
    if weights and any(abs(w-1.0)>1e-9 for w in weights):
        wtxt = "_".join([f"{w:.2f}".rstrip("0").rstrip(".") for w in weights]); base += f"__w_{wtxt}"
    return base + f"__{method}"

def compute_scores(df: pd.DataFrame, cols: List[str], weights: List[float]|None, method: str)->pd.Series:
    X = df[cols].copy()
    if method=="zscore_mean":
        Z = X.groupby(df["date"]).transform(zscore)
        if weights: 
            w = np.array(weights)/(np.sum(weights)+1e-12); s=(Z*w).sum(axis=1)
        else:
            s=Z.mean(axis=1)
    elif method=="rank_mean":
        R = X.groupby(df["date"]).rank(pct=True)
        if weights: 
            w = np.array(weights)/(np.sum(weights)+1e-12); s=(R*w).sum(axis=1)
        else:
            s=R.mean(axis=1)
    else:
        raise ValueError(f"未知的 method: {method}")
    return s

def expand_grid(spec: dict)->List[dict]:
    out=[]
    for item in spec.get("combos",[]):
        cols_list=item["factors"]
        weights_list=item.get("weights",[None]*len(cols_list))
        method=item.get("combine","zscore_mean")
        grid=item.get("grid",{})
        grid_keys=sorted(list(grid.keys()))
        grid_vals=[grid[k] for k in grid_keys] if grid_keys else [[]]
        for cols, ww in zip(cols_list, weights_list):
            if not isinstance(cols,list): cols=[cols]
            if ww is not None and not isinstance(ww,list): ww=[ww]
            for combo_vals in itertools.product(*grid_vals) if grid_keys else [()]:
                out.append({"factors":cols,"weights":ww,"combine":method,"overrides":dict(zip(grid_keys,combo_vals)) if grid_keys else {}})
    return out

def run_one_script_engine(factors_path:str, base_cfg:dict, run_cfg:dict, out_dir:str):
    cfg=dict(base_cfg); cfg.update(run_cfg.get("overrides") or {}); cfg["score_col"]=run_cfg["score_col"]
    cfg_path=os.path.join(out_dir,"config.yaml"); save_yaml(cfg,cfg_path)
    backtest_py=os.path.join(REPO_ROOT,"backtest","longonly_topN.py")
    cmd=[sys.executable, backtest_py,"--factors",factors_path,"--out-dir",out_dir,"--config",cfg_path]
    print("[RUN]"," ".join(cmd)); subprocess.run(cmd, check=True)

def simple_builtin_engine(fac: pd.DataFrame, score_col:str, cfg:dict, out_dir:str):
    df=fac[["date","symbol","ret",score_col]].copy(); df["date"]=pd.to_datetime(df["date"])
    df=df.sort_values(["date",score_col], ascending=[True,False])
    topN=int(cfg.get("topN",50))
    weekly=df.groupby("date").head(topN)
    daily_ret=weekly.groupby("date")["ret"].mean()
    nav=(1+daily_ret.fillna(0.0)).cumprod()
    nav.to_csv(os.path.join(out_dir,"nav.csv"), header=["nav"])
    daily_ret.to_csv(os.path.join(out_dir,"ret.csv"), header=["ret"])

def worker_run(run:dict, args, base_cfg:dict, augmented_path:str, root_out:str):
    cols=run["factors"]; weights=run.get("weights"); method=run.get("combine","zscore_mean")
    score_col=build_combo_name(cols,weights,method)
    run_name="__".join([score_col]+[f"{k}={v}" for k,v in (run.get("overrides") or {}).items()]).replace(" ","")
    out_dir=os.path.join(root_out,run_name); os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir,"meta.json"),"w",encoding="utf-8") as f:
        json.dump({"factors":cols,"weights":weights,"combine":method,"overrides":run.get("overrides")}, f, ensure_ascii=False, indent=2)

    try:
        if args.engine=="script":
            run_one_script_engine(augmented_path, base_cfg, {"score_col":score_col,"overrides":run.get("overrides")}, out_dir)
        else:
            fac=pd.read_parquet(augmented_path)
            simple_builtin_engine(fac, score_col, base_cfg | (run.get("overrides") or {}), out_dir)
    except Exception as e:
        with open(os.path.join(out_dir,"_error.txt"),"w",encoding="utf-8") as f: f.write(str(e))
        print(f"[ERROR] {run_name}: {e}")
        return

    if args.make_report:
        try:
            nav_path=os.path.join(out_dir,"nav.csv")
            if os.path.exists(nav_path):
                cmd=[sys.executable, os.path.join(REPO_ROOT,"scripts","make_report.py"),
                     "--nav-csv", nav_path, "--date-col","date","--value-col","nav",
                     "--out-dir", out_dir, "--freq", args.freq, "--title", run_name, "--name", run_name]
                subprocess.run(cmd, check=True)
        except Exception as e:
            with open(os.path.join(out_dir,"_report_error.txt"),"w",encoding="utf-8") as f: f.write(str(e))

def main():
    args=parse_args()
    os.makedirs(args.out_root, exist_ok=True)
    batch_root=os.path.join(args.out_root, datetime.now().strftime("%Y%m%d-%H%M%S")); os.makedirs(batch_root, exist_ok=True)
    base_cfg=load_yaml(args.base_config); spec=load_yaml(args.combos); runs=expand_grid(spec)

    augmented_path=os.path.join(batch_root,"alpha_factors__augmented.parquet")
    fac=pd.read_parquet(args.factors_path); fac["date"]=pd.to_datetime(fac["date"])
    if args.chunk_months and args.chunk_months>0:
        # 分段計算（降低 RAM）
        new_cols=[]
        for r in runs:
            col=build_combo_name(r["factors"], r.get("weights"), r.get("combine","zscore_mean"))
            new_cols.append(col); fac[col]=np.nan
        min_d,max_d=fac["date"].min(), fac["date"].max()
        cur=min_d
        from pandas.tseries.offsets import MonthBegin
        while cur<=max_d:
            nxt=cur+MonthBegin(args.chunk_months)
            part_idx=fac.index[(fac["date"]>=cur)&(fac["date"]<nxt)]
            part=fac.loc[part_idx,:].copy()
            for r in runs:
                col=build_combo_name(r["factors"], r.get("weights"), r.get("combine","zscore_mean"))
                part[col]=compute_scores(part, r["factors"], r.get("weights"), r.get("combine","zscore_mean"))
            fac.loc[part_idx,new_cols]=part[new_cols]
            cur=nxt
        fac.to_parquet(augmented_path, index=False)
    else:
        for r in runs:
            col=build_combo_name(r["factors"], r.get("weights"), r.get("combine","zscore_mean"))
            fac[col]=compute_scores(fac, r["factors"], r.get("weights"), r.get("combine","zscore_mean"))
        fac.to_parquet(augmented_path, index=False)

    if args.jobs<=1:
        for r in runs: worker_run(r, args, base_cfg, augmented_path, batch_root)
    else:
        from multiprocessing import Pool
        with Pool(processes=args.jobs) as pool:
            pool.starmap(worker_run, [(r,args,base_cfg,augmented_path,batch_root) for r in runs])

    print("[DONE] 全部任務完成。輸出目錄：", batch_root)

if __name__=="__main__":
    main()
  ''',

  "configs/batch_backtest.example.yaml": r'''
combos:
  - factors:
      - ["composite_score"]
      - ["mom_252_21", "vol_20"]
    weights:
      - null
      - [0.7, 0.3]
    combine: zscore_mean
    grid:
      topN: [30, 50]
      fees_bps: [2.5, 10]
  ''',

  "templates/report_template.md.j2": r'''
# {{ title }}

- 產出時間：{{ generated_at }}

## 策略指標
- 期間：{{ strategy.start_date }} → {{ strategy.end_date }}（{{ strategy.periods }} 筆）
- CAGR：{{ '%.2f%%' % (strategy.cagr*100) if strategy.cagr==strategy.cagr else '—' }}
- 總報酬：{{ '%.2f%%' % (strategy.total_return*100) if strategy.total_return==strategy.total_return else '—' }}
- 波動（年化）：{{ '%.2f%%' % (strategy.vol*100) if strategy.vol==strategy.vol else '—' }}
- Sharpe：{{ '%.2f' % strategy.sharpe if strategy.sharpe==strategy.sharpe else '—' }}
- Sortino：{{ '%.2f' % strategy.sortino if strategy.sortino==strategy.sortino else '—' }}
- 最大回撤：{{ '%.2f%%' % (strategy.max_dd*100) if strategy.max_dd==strategy.max_dd else '—' }}
- Calmar：{{ '%.2f' % strategy.calmar if strategy.calmar==strategy.calmar else '—' }}
- 勝率：{{ '%.2f%%' % (strategy.win_rate*100) if strategy.win_rate==strategy.win_rate else '—' }}
- 單日最好 / 最差：{{ '%.2f%%' % (strategy.best_day*100) if strategy.best_day==strategy.best_day else '—' }} / {{ '%.2f%%' % (strategy.worst_day*100) if strategy.worst_day==strategy.worst_day else '—' }}

{% if benchmark %}
## Benchmark 指標
- 期間：{{ benchmark.start_date }} → {{ benchmark.end_date }}（{{ benchmark.periods }} 筆）
- CAGR：{{ '%.2f%%' % (benchmark.cagr*100) if benchmark.cagr==benchmark.cagr else '—' }}
- 總報酬：{{ '%.2f%%' % (benchmark.total_return*100) if benchmark.total_return==benchmark.total_return else '—' }}
- 波動（年化）：{{ '%.2f%%' % (benchmark.vol*100) if benchmark.vol==benchmark.vol else '—' }}
- Sharpe：{{ '%.2f' % benchmark.sharpe if benchmark.sharpe==benchmark.sharpe else '—' }}
- 最大回撤：{{ '%.2f%%' % (benchmark.max_dd*100) if benchmark.max_dd==benchmark.max_dd else '—' }}
{% endif %}

## 圖表
![NAV / Benchmark]({{ images.nav_or_vs }})
![Drawdown]({{ images.drawdown }})
![Rolling Sharpe]({{ images.rolling_sharpe }})
  ''',

  "README_REPORTING.md": r'''
# tw-alpha-reporting（報表輸出 + 批量回測）
需求：Python 3.10+、pandas、numpy、matplotlib、pyyaml（可選 jinja2）

## 單次報表
(.venv) python scripts/make_report.py ^
  --nav-csv G:\AI\datahub\alpha\backtests\topN_50_M\nav.csv ^
  --date-col date --value-col nav ^
  --benchmark-csv G:\AI\data\bench\taiex_close.csv --bench-type close ^
  --bench-date-col date --bench-value-col close ^
  --out-dir G:\AI\datahub\alpha\backtests\topN_50_M ^
  --freq D ^
  --title "TopN=50 Composite"

## 批量回測（自動報表）
(.venv) python scripts/batch_backtest.py ^
  --factors-path G:\AI\datahub\alpha\alpha_factors.parquet ^
  --base-config configs\backtest_topN_fixed.yaml ^
  --combos configs\batch_backtest.example.yaml ^
  --out-root G:\AI\datahub\alpha\backtests\grid_test ^
  --jobs 2 ^
  --engine script ^
  --make-report ^
  --freq D
  '''
}

def write_file(root, rel, content, force=False):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if (not force) and os.path.exists(path):
        print(f"[skip] 已存在：{rel}")
        return
    with open(path, "w", encoding="utf-8") as f:
        f.write(content.lstrip("\n"))
    print(f"[ok] 寫入：{rel}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="覆寫已存在的檔案")
    args = ap.parse_args()

    repo_root = os.getcwd()
    for rel, content in FILES.items():
        write_file(repo_root, rel, content, force=args.force)

    print("\n完成 ✅")
    print("接著可執行：")
    print("  python scripts/make_report.py --help")
    print("  python scripts/batch_backtest.py --help")

if __name__ == "__main__":
    main()
