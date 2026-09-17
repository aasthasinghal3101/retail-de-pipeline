"""
build_index.py
---------------
Builds the retrieval index the NL query assistant (ask_data.py) searches
over. Two document pools are indexed together with TF-IDF:

  1. Query templates (genai_assistant/query_library.py) — curated
     question -> SQL pairs, indexed on their example phrasings.
  2. Live schema documents — table + column metadata pulled directly from
     the warehouse's information_schema, so the assistant's retrieval
     stays in sync with whatever the dbt marts actually look like today
     (if a column is renamed or a mart is added, re-running this script
     picks it up with no code change).

This is a genuine retrieve-then-generate pipeline: TF-IDF/cosine-similarity
is used here instead of a neural embedding model so the whole assistant
runs fully offline with no API key or model download — swap
`Retriever._vectorize` for a sentence-transformers or OpenAI/Anthropic
embedding call to upgrade retrieval quality without changing anything
downstream.

Usage:
    python genai_assistant/build_index.py
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path

import duckdb
from sklearn.feature_extraction.text import TfidfVectorizer

from query_library import QUERY_LIBRARY

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"
INDEX_DIR = Path(__file__).resolve().parent / "index"


def fetch_schema_documents() -> list[dict]:
    """Pulls live column metadata for the marts schema so the assistant
    always has an accurate answer to 'what data do you have?' questions."""
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    rows = con.execute(
        """
        select table_name, column_name, data_type
        from information_schema.columns
        where table_schema in ('main_marts', 'main_staging')
        order by table_name, ordinal_position
        """
    ).fetchall()
    con.close()

    tables: dict[str, list[str]] = {}
    for table_name, column_name, data_type in rows:
        tables.setdefault(table_name, []).append(f"{column_name} ({data_type})")

    docs = []
    for table_name, columns in tables.items():
        docs.append(
            {
                "doc_type": "schema",
                "id": table_name,
                "text": f"table {table_name} columns: {', '.join(columns)}",
                "columns": columns,
            }
        )
    return docs


def fetch_template_documents() -> list[dict]:
    docs = []
    for tmpl in QUERY_LIBRARY:
        text = " ".join(tmpl.example_questions) + " " + tmpl.description
        docs.append({"doc_type": "template", "id": tmpl.id, "text": text})
    return docs


def main() -> None:
    if not WAREHOUSE_PATH.exists():
        raise FileNotFoundError(
            f"{WAREHOUSE_PATH} not found — run the pipeline "
            "(orchestration/run_pipeline.py) before building the index."
        )

    template_docs = fetch_template_documents()
    schema_docs = fetch_schema_documents()
    all_docs = template_docs + schema_docs

    corpus = [d["text"] for d in all_docs]
    vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2))
    doc_vectors = vectorizer.fit_transform(corpus)

    INDEX_DIR.mkdir(exist_ok=True)
    with open(INDEX_DIR / "vectorizer.pkl", "wb") as f:
        pickle.dump(vectorizer, f)
    with open(INDEX_DIR / "doc_vectors.pkl", "wb") as f:
        pickle.dump(doc_vectors, f)
    with open(INDEX_DIR / "documents.json", "w") as f:
        json.dump(all_docs, f, indent=2)

    print(f"Indexed {len(template_docs)} query templates + {len(schema_docs)} schema documents")
    print(f"Index written to: {INDEX_DIR}")


if __name__ == "__main__":
    main()
