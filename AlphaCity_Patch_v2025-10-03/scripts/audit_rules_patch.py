# 用途：比對現有 rules.yaml 與 rules_patch.yaml，輸出合併預覽到 reports/rules_merged_preview.yaml
import argparse, sys, pathlib, yaml
from copy import deepcopy

def merge(a, b):
    # 深度合併：b 覆蓋/補充 a
    if isinstance(a, dict) and isinstance(b, dict):
        out = deepcopy(a)
        for k, v in b.items():
            if k in out:
                out[k] = merge(out[k], v)
            else:
                out[k] = deepcopy(v)
        return out
    return deepcopy(b)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rules", default="rules.yaml")
    p.add_argument("--patch", default="rules/rules_patch.yaml")
    p.add_argument("--out",   default="reports/rules_merged_preview.yaml")
    args = p.parse_args()

    base = pathlib.Path(args.rules)
    patch = pathlib.Path(args.patch)
    outp  = pathlib.Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with base.open("r", encoding="utf-8") as f:
        base_yaml = yaml.safe_load(f) or {}
    with patch.open("r", encoding="utf-8") as f:
        patch_yaml = yaml.safe_load(f) or {}

    merged = merge(base_yaml, patch_yaml)
    with outp.open("w", encoding="utf-8") as f:
        yaml.safe_dump(merged, f, sort_keys=False, allow_unicode=True)

    print(f"[OK] Preview written to {outp}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
