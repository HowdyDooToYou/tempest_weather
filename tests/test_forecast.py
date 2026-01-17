import unittest

import pandas as pd

from src.forecast import parse_tempest_forecast


SAMPLE_PAYLOAD = {
    "timezone": "America/New_York",
    "forecast": {
        "hourly": [
            {"time": 1608735600, "conditions": "Clear", "air_temperature": 57, "feels_like": 56, "precip_probability": 0},
            {"time": 1608739200, "conditions": "Clear", "air_temperature": 59, "feels_like": 58, "precip_probability": 10},
        ],
        "daily": [
            {
                "day_start_local": 1608699600,
                "conditions": "Clear",
                "air_temp_high": 73,
                "air_temp_low": 39,
                "precip_probability": 10,
                "sunrise": 1608639322,
                "sunset": 1608676362,
            },
            {
                "day_start_local": 1608786000,
                "conditions": "Cloudy",
                "air_temp_high": 70,
                "air_temp_low": 41,
                "precip_probability": 20,
                "sunrise": 1608725780,
                "sunset": 1608762740,
            },
        ],
    },
}


class ForecastParseTest(unittest.TestCase):
    def test_parse_tempest_forecast(self):
        hourly, daily, tz = parse_tempest_forecast(SAMPLE_PAYLOAD, tz_fallback="UTC")
        self.assertEqual(tz, "America/New_York")
        self.assertIsNotNone(hourly)
        self.assertFalse(hourly.empty)
        self.assertIsNotNone(daily)
        self.assertFalse(daily.empty)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(hourly["time"]))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(daily["day_start_local"]))
        self.assertIn("sunrise", daily.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(daily["sunrise"]))
        self.assertIn("sunset", daily.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(daily["sunset"]))


if __name__ == "__main__":
    unittest.main()
