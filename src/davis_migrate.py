from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "tempest.db"

def main():
    print("Deprecated: davis_migrate.py no longer manages tables.")
    print(f"No changes applied to DB at {DB_PATH}")

if __name__ == "__main__":
    main()
