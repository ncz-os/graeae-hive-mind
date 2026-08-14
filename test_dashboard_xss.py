"""The dashboard must not render worker-controlled telemetry as HTML.

Host metadata is supplied by whoever holds a worker bearer token. The dashboard
stores the admin bus token in `sessionStorage`, so a single unescaped field is
enough to exfiltrate it: put `<img src=x onerror=...>` in `metadata.specs.ram_gb`,
wait for an operator to open the dashboard, and the script runs with the admin
token in reach.

These tests read dashboard.html as text rather than driving a browser: the
defect is a source-level property (which sink each value reaches), and a text
assertion is what will still be checked in CI on a box with no browser.
"""

import pathlib
import re

DASHBOARD = pathlib.Path(__file__).with_name("dashboard.html")


def _source() -> str:
    return DASHBOARD.read_text(encoding="utf-8")


def test_text_and_numeric_helpers_exist():
    src = _source()
    assert "function tdText(" in src, "a text-node cell helper must exist"
    assert "function num(" in src, "a numeric-coercion helper must exist"
    # num() must reject non-finite input rather than pass the string through.
    assert "Number.isFinite" in src, "num() must validate, not just concatenate"


def test_worker_controlled_telemetry_never_reaches_innerhtml():
    """Every `td(...)` that interpolates a value must escape or coerce it.

    `td()` assigns to innerHTML for non-Node input, so a raw interpolation is a
    live XSS sink. Markup templates are fine as long as the *values* inside them
    go through esc() or num().
    """
    src = _source()
    offenders = []
    for lineno, line in enumerate(src.splitlines(), 1):
        if "td(" not in line:
            continue
        # only calls that interpolate something dynamic
        if "${" not in line and "+" not in line:
            continue
        if re.search(r"\b(esc|num|tdText|pbar|loadBar)\s*\(", line):
            continue
        if "toFixed(" in line or "toLocaleString(" in line:
            continue
        if re.search(r"td\(\s*''\s*\)", line):
            continue
        offenders.append(f"{lineno}: {line.strip()[:100]}")
    assert not offenders, "unescaped values reach td()'s innerHTML sink:\n" + "\n".join(offenders)


def test_the_specific_reported_sink_is_fixed():
    """Regression: metadata.specs.ram_gb was rendered straight into innerHTML."""
    src = _source()
    assert "td(h.ram_gb!=null?h.ram_gb+'GB':'-')" not in src, (
        "ram_gb is worker-controlled and must not be concatenated into innerHTML"
    )
    assert "tdText(h.ram_gb" in src, "ram_gb must render through a text node"
