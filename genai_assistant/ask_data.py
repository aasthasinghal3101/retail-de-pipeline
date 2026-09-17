"""
ask_data.py
-----------
"Ask your data" — a retrieval-augmented natural-language query assistant
over the dbt marts.

Pipeline (classic RAG shape):
    question
        -> RETRIEVE: TF-IDF cosine similarity against query_library.py's
           curated question/SQL pairs + live warehouse schema docs
        -> AUGMENT:  fill the retrieved SQL template's slots (e.g. "top N")
           parsed out of the question
        -> EXECUTE:  run the resulting SQL against the warehouse
        -> GENERATE: render the result as a natural-language answer

By default GENERATE is a deterministic template fill, so the whole
assistant runs offline with zero API cost. Passing --use-llm (with
ANTHROPIC_API_KEY set) swaps that last step for an actual Claude call that
turns the retrieved context + SQL result into a more natural response —
see `generate_with_llm()` below for the integration point.

Usage:
    python genai_assistant/ask_data.py "who are our top 5 customers"
    python genai_assistant/ask_data.py "revenue by region"
    python genai_assistant/ask_data.py --use-llm "which region should we focus growth on?"
"""
from __future__ import annotations

import argparse
import json
import pickle
import re
import sys
from pathlib import Path

import duckdb
from sklearn.metrics.pairwise import cosine_similarity

from query_library import QUERY_LIBRARY

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"
INDEX_DIR = Path(__file__).resolve().parent / "index"

SIMILARITY_THRESHOLD = 0.12  # below this, admit we don't have a good match
DEFAULT_TOP_N = 10


def load_index():
    with open(INDEX_DIR / "vectorizer.pkl", "rb") as f:
        vectorizer = pickle.load(f)
    with open(INDEX_DIR / "doc_vectors.pkl", "rb") as f:
        doc_vectors = pickle.load(f)
    with open(INDEX_DIR / "documents.json") as f:
        documents = json.load(f)
    return vectorizer, doc_vectors, documents


def retrieve(question: str, vectorizer, doc_vectors, documents) -> tuple[dict, float]:
    q_vec = vectorizer.transform([question])
    sims = cosine_similarity(q_vec, doc_vectors).flatten()
    best_idx = sims.argmax()
    return documents[best_idx], float(sims[best_idx])


def extract_n(question: str, default: int = DEFAULT_TOP_N) -> int:
    match = re.search(r"\btop\s+(\d+)\b|\b(\d+)\s+(?:customers|days|products)\b", question.lower())
    if match:
        value = match.group(1) or match.group(2)
        return int(value)
    return default


def format_rows(rows: list[tuple], columns: list[str]) -> str:
    if not rows:
        return "  (no rows returned)"
    lines = []
    for row in rows:
        parts = [f"{col}={val}" for col, val in zip(columns, row)]
        lines.append("  - " + ", ".join(parts))
    return "\n".join(lines)


def generate_with_llm(question: str, sql: str, rows: list[tuple], columns: list[str]) -> str:
    """Integration point for a real LLM-backed generation step.

    Swaps the deterministic template-fill in `answer()` for a call to
    Claude that turns (question, SQL, result rows) into a natural,
    analyst-style answer. Requires `pip install anthropic` and
    ANTHROPIC_API_KEY — not required to run the rest of this project.
    """
    try:
        import anthropic  # noqa: F401  (imported lazily — optional dependency)
    except ImportError as exc:
        raise RuntimeError(
            "generate_with_llm() requires `pip install anthropic` and an ANTHROPIC_API_KEY."
        ) from exc

    client = anthropic.Anthropic()
    result_preview = format_rows(rows[:20], columns)
    prompt = (
        f"A user asked a data analyst assistant: \"{question}\"\n\n"
        f"The assistant ran this SQL against the warehouse:\n{sql}\n\n"
        f"Result:\n{result_preview}\n\n"
        "Write a concise, natural-language answer (2-4 sentences) a business "
        "stakeholder would find useful. Cite the actual numbers."
    )
    message = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def answer_from_template(question: str, template_id: str, n: int) -> str:
    tmpl = next(t for t in QUERY_LIBRARY if t.id == template_id)
    sql = tmpl.sql.format(n=n) if "{n}" in tmpl.sql else tmpl.sql

    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    cursor = con.execute(sql)
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    con.close()

    if len(columns) == 1 and len(rows) == 1:
        rendered_rows = str(rows[0][0])
    else:
        rendered_rows = format_rows(rows, columns)

    return tmpl.answer_template.format(rows=rendered_rows, n=n), sql, rows, columns


def ask(question: str, use_llm: bool = False) -> None:
    if not INDEX_DIR.exists() or not (INDEX_DIR / "vectorizer.pkl").exists():
        print("Index not found — run `python genai_assistant/build_index.py` first.", file=sys.stderr)
        sys.exit(1)

    vectorizer, doc_vectors, documents = load_index()
    best_doc, score = retrieve(question, vectorizer, doc_vectors, documents)

    print(f"Q: {question}")
    print(f"[retrieved: {best_doc['doc_type']}:{best_doc['id']}  similarity={score:.3f}]")

    if score < SIMILARITY_THRESHOLD or best_doc["doc_type"] != "template":
        available = ", ".join(t.description for t in QUERY_LIBRARY)
        print(
            "I don't have a confident answer for that yet. Things I can answer:\n"
            f"  {available}"
        )
        return

    n = extract_n(question)
    text_answer, sql, rows, columns = answer_from_template(question, best_doc["id"], n)

    if use_llm:
        try:
            text_answer = generate_with_llm(question, sql, rows, columns)
        except RuntimeError as exc:
            print(f"[--use-llm requested but unavailable: {exc}]")
            print("[falling back to templated answer]")

    print(f"A: {text_answer}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ask a natural-language question about the retail data.")
    parser.add_argument("question", nargs="+", help="Your question, e.g. 'top 5 customers by spend'")
    parser.add_argument("--use-llm", action="store_true", help="Use Claude for the generation step (requires ANTHROPIC_API_KEY).")
    args = parser.parse_args()
    ask(" ".join(args.question), use_llm=args.use_llm)


if __name__ == "__main__":
    main()
