
[2025-10-16] S1(Date-ID) 主檔落地 vX：
• Root 解析強化：ALPHACITY_ROOT 優先，其次向上尋 configs+tools，常見路徑備援；log 落在 <root>\reports\
• Group=ALL 不截字；與 Universe 交集；IDs 檢核行數與 sample 輸出
• 逐窗 + 402 退避重試：402→sleep(backoff)→rpm//2→同窗重試（上限）→成功 reset backoff
• 提速：連續成功 RampEveryWins 個窗就 +StepRPM，上限 MaxRPM
• 只監聽 402，429 忽略
• 引擎定位：Run-FullMarket-DateID-MaxRate.ps1（正名）→ fallback shim（DateIDMaxRate）
• 日誌：fullmarket_maxrate_<Tag>_<ts>.log；每窗印 "=== s → e === IDs~N rpm=R"、[Backoff]、[Ramp]、結尾 "S1 batch DONE"
• 驗證成果（S1_verify）：IDs~2514，rpm 8→12→16；最新窗 "2025-10-16 → 2025-10-17"
• 待遵循：外層啟動亦採 -ExecutionPolicy Bypass；長跑前先 build_universe.py --drop-empty

