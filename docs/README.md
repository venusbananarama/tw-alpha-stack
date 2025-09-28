# AlphaCity pkgD v6.3 patch1

## 更新內容
- 修正 `Register-AlphaCity-VerifyTasks.ps1` 首行多餘字元 `\` 導致無法執行的問題。
- 確保檔案以 `param(...)` 開頭。

## 衝突處理
1. 建議先清除舊任務：
   ```powershell
   .\pkgD_tasks_v6_3\Unregister-AlphaCity-VerifyTasks.ps1
   ```
2. 再執行註冊：
   ```powershell
   .\pkgD_tasks_v6_3\Register-AlphaCity-VerifyTasks.ps1 -Root 'G:\AI\tw-alpha-stack'
   ```

## 注意
- 舊版 `pkgD_tasks_v6` 資料夾可刪除，避免混淆。
- 其他模組（回測、因子計算、報表）不受影響。
