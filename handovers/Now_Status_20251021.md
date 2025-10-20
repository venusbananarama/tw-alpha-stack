# Now_Status 20251021
- Parser All Green
- Root non-shim=0
- roots map rows: 
65
- per-folder: fullmarket=3  dateid=7  daily=6  gate=6  universe=4  orchestrator=15  devops=13  repair=8  _legacy=1

### sample mapping (top 10)

old_name                    new_target
--------                    ----------
AckLive.ps1                 C:\AI\tw-alpha-stack\tools\orchestrator\AckLive.ps1
AlphaCity.Refactor.ps1      C:\AI\tw-alpha-stack\tools\devops\AlphaCity.Refactor.ps1
Backfill-Dividend-Force.ps1 C:\AI\tw-alpha-stack\tools\daily\Backfill-Dividend-Force.ps1
Backfill-FullMarket.ps1     C:\AI\tw-alpha-stack\tools\fullmarket\Backfill-FullMarket.ps1
Backfill-RatePlan.fast.ps1  C:\AI\tw-alpha-stack\tools\fullmarket\Run-FullMarket-DateID-MaxRate.ps1
Backfill-RatePlan.ps1       C:\AI\tw-alpha-stack\tools\fullmarket\Run-FullMarket-DateID-MaxRate.ps1
Build-IDsFromUniverse.ps1   C:\AI\tw-alpha-stack\tools\universe\Build-IDsFromUniverse.ps1
Build-Universe-Failsafe.ps1 C:\AI\tw-alpha-stack\tools\universe\Build-Universe-Failsafe.ps1
check_env.ps1               C:\AI\tw-alpha-stack\tools\devops\check_env.ps1
Check-CanonicalLayout.ps1   C:\AI\tw-alpha-stack\tools\repair\Check-CanonicalLayout.ps1


### Entrypoint & Orchestrator Checks
- Run-Max-Recent.ps1 guard: OK
- Run-WFGate.ps1 guard: OK
- Orchestrator self_call: False
