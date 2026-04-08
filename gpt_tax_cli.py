import argparse
from pathlib import Path
from openai import OpenAI

# Wir analysieren NUR taxtrack
ALLOWED_DIR = "taxtrack"
INCLUDE_EXT = {".py", ".md"}
EXCLUDE_DIRS = {".git", ".venv", "__pycache__", ".pytest_cache"}
MAX_CHARS = 8000

def load_context(root: Path) -> str:
    chunks = []
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        if any(part in EXCLUDE_DIRS for part in p.parts):
            continue
        if p.suffix not in INCLUDE_EXT:
            continue

        text = p.read_text(encoding="utf-8", errors="ignore")
        if len(text) > MAX_CHARS:
            text = text[:MAX_CHARS] + "\n...<TRUNCATED>"

        chunks.append(f"\n### FILE: {p.relative_to(root)}\n{text}")

    return "\n".join(chunks)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ask", required=True)
    args = parser.parse_args()

    project_root = Path(ALLOWED_DIR)
    context = load_context(project_root)

    client = OpenAI()

    response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {
                "role": "system",
                "content": (
                    "Du bist ein erfahrener Python-Architekt für Steuer- "
                    "und Finanztools. Analysiere den gegebenen Code nüchtern."
                ),
            },
            {
                "role": "user",
                "content": f"CODEBASE:\n{context}\n\nAUFGABE:\n{args.ask}",
            },
        ],
    )

    print(response.output_text)

if __name__ == "__main__":
    main()
