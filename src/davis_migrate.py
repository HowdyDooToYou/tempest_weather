import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.getenv("TEMPEST_DB_PATH", str(PROJECT_ROOT / "data" / "tempest.db")))
if not DB_PATH.is_absolute():
    DB_PATH = PROJECT_ROOT / DB_PATH

def main():
    print("Deprecated: davis_migrate.py no longer manages tables.")
    print(f"No changes applied to DB at {DB_PATH}")

if __name__ == "__main__":
    main()
