from contextlib import closing
from pathlib import Path
import unittest

from src.config_store import (
    connect,
    get_bool,
    get_config,
    get_float,
    set_bool,
    set_config,
    set_float,
)


class ConfigStoreTest(unittest.TestCase):
    def test_round_trip_config(self):
        data_dir = Path.cwd() / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        db_path = data_dir / "test_config_store.db"
        if db_path.exists():
            db_path.unlink()

        with closing(connect(db_path)) as conn:
            set_config(conn, "sample_key", "value")
            self.assertEqual(get_config(conn, "sample_key"), "value")

            set_bool(conn, "override_location_enabled", True)
            self.assertTrue(get_bool(conn, "override_location_enabled"))
            set_bool(conn, "override_location_enabled", False)
            self.assertFalse(get_bool(conn, "override_location_enabled"))

            set_float(conn, "station_lat_override", 12.3456)
            self.assertAlmostEqual(get_float(conn, "station_lat_override"), 12.3456)

        with closing(connect(db_path)) as conn:
            self.assertEqual(get_config(conn, "sample_key"), "value")
            self.assertFalse(get_bool(conn, "override_location_enabled"))
            self.assertAlmostEqual(get_float(conn, "station_lat_override"), 12.3456)

        for suffix in ("", "-wal", "-shm"):
            candidate = Path(f"{db_path}{suffix}")
            if candidate.exists():
                candidate.unlink()


if __name__ == "__main__":
    unittest.main()
