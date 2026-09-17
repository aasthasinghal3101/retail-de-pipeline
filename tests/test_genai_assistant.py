"""Unit tests for the GenAI query assistant's non-warehouse-dependent logic."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "genai_assistant"))

from ask_data import extract_n, format_rows  # noqa: E402
from query_library import QUERY_LIBRARY  # noqa: E402


def test_extract_n_finds_top_n_pattern():
    assert extract_n("show me the top 5 customers") == 5
    assert extract_n("top 25 products") == 25


def test_extract_n_defaults_when_absent():
    assert extract_n("what is the average order value", default=10) == 10


def test_format_rows_empty():
    assert "no rows" in format_rows([], ["a", "b"]).lower()


def test_format_rows_renders_columns():
    out = format_rows([("East", 100)], ["region", "revenue"])
    assert "region=East" in out
    assert "revenue=100" in out


def test_query_library_ids_are_unique():
    ids = [t.id for t in QUERY_LIBRARY]
    assert len(ids) == len(set(ids))


def test_query_library_every_template_has_examples():
    for tmpl in QUERY_LIBRARY:
        assert len(tmpl.example_questions) >= 2, f"{tmpl.id} needs more example phrasings"
        assert tmpl.sql.strip(), f"{tmpl.id} has empty SQL"
