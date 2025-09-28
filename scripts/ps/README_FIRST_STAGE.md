
# First Stage - Rigorous Checklist (AlphaCity)
Adds:
- Progress tracking (state/finmind_progress.json)
- Metrics CSV (metrics/ingest_summary_*.csv)
- Silver schema contracts (schemas/datasets_schema.yaml)
- DuckDB DQ checks (scripts/dq_check.sql)

Place:
- scripts/ingest_utils.py
- schemas/datasets_schema.yaml
- scripts/dq_check.sql

Use:
from ingest_utils import ProgressTracker, MetricsWriter, SchemaValidator
prog = ProgressTracker('state/finmind_progress.json')
metrics = MetricsWriter('metrics')

# After a successful task:
prog.mark_dataset_date(dataset, date_str, ok=True); prog.save()
metrics.add(dataset=dataset, date=date_str, rows=rows, secs=elapsed, retries=retries, mode=('symbols' if symbol else 'all'))

# Before writing Silver:
import yaml
schema_all = yaml.safe_load(open('schemas/datasets_schema.yaml','r',encoding='utf-8'))
validator = SchemaValidator(schema_all[dataset])
df = validator.coerce_types(df)
missing = validator.validate_columns(df)
if missing: raise ValueError(f'Missing required columns: {missing}')

Run DQ:
.duckdb> .read scripts/dq_check.sql
