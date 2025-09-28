import sys, importlib
mods = ["pandas","numpy","yaml","pyarrow","matplotlib","openpyxl"]
print("Python:", sys.version)
print("Executable:", sys.executable)
for m in mods:
    try:
        mod = importlib.import_module(m)
        ver = getattr(mod, "__version__", "?")
        print(f"[OK] {m} {ver}")
    except Exception as e:
        print(f"[MISS] {m} -> {e}")
