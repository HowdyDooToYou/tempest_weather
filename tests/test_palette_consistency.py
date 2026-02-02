import ast
import re
from pathlib import Path

HEX_RE = re.compile(r"^#[0-9a-fA-F]{6}$")
REQUIRED_KEYS = {
    "scheme",
    "mode",
    "bg",
    "surface",
    "surface_2",
    "surface_3",
    "surface_4",
    "border",
    "text_primary",
    "text_secondary",
    "text_muted",
    "accent",
    "accent2",
    "accent3",
    "status_ok",
    "status_warn",
    "status_bad",
    "status_idle",
}
COLOR_KEYS = REQUIRED_KEYS - {"scheme", "mode"}


def _load_palette_options():
    source = Path("dashboard.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "palette_options":
                return ast.literal_eval(node.value)
    raise AssertionError("palette_options not found in dashboard.py")


def _hex_luminance(value: str) -> float:
    value = value.lstrip("#")
    r = int(value[0:2], 16) / 255.0
    g = int(value[2:4], 16) / 255.0
    b = int(value[4:6], 16) / 255.0
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def test_palette_schema_and_colors():
    palettes = _load_palette_options()
    assert palettes, "No palettes found"
    for name, data in palettes.items():
        missing = REQUIRED_KEYS - set(data.keys())
        assert not missing, f"Palette {name} missing keys: {sorted(missing)}"
        assert data["mode"] in ("dark", "light"), f"Palette {name} has invalid mode"
        assert isinstance(data["scheme"], str) and data["scheme"], f"Palette {name} missing scheme"
        for key in COLOR_KEYS:
            value = data[key]
            assert isinstance(value, str), f"Palette {name} key {key} is not a string"
            assert HEX_RE.match(value), f"Palette {name} key {key} is not a hex color"


def test_palette_contrast_guardrails():
    palettes = _load_palette_options()
    for name, data in palettes.items():
        bg_luma = _hex_luminance(data["bg"])
        text_luma = _hex_luminance(data["text_primary"])
        if data["mode"] == "light":
            assert bg_luma >= 0.7, f"Palette {name} light bg too dark"
            assert text_luma <= 0.35, f"Palette {name} light text too light"
        else:
            assert bg_luma <= 0.25, f"Palette {name} dark bg too light"
            assert text_luma >= 0.7, f"Palette {name} dark text too dark"
