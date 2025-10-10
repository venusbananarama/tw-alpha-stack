# 發佈前自檢清單
- Preflight 全 PASS（freshness、schema）
- Gate 新鍵已合併（DSR/PSR/t、replay_mae_bps_max、POV/limit_protect）
- 若無改 universe.yaml → 不重建投資池
- 回放偏差 ≤ 2 bps
- Sharpe_after_costs 降幅 ≤ 0.3
