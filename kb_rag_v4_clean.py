from pathlib import Path
from collections import Counter
from datetime import datetime
import json
import re

from ftfy import fix_text

# ========================
# CONFIG
# ========================

KB_ROOT = Path("/srv/data/kb_projects")
MIN_REPEAT = 5           # mínimo para considerar linha repetida
MAX_LINE_LEN = 120       # evita remover cláusulas longas

# ========================
# CLEANING STEPS
# ========================

def normalize_text(text: str) -> str:
    text = fix_text(text)

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # espaços múltiplos
    text = re.sub(r"[ \t]+", " ", text)

    # linhas vazias excessivas
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text


def detect_repeated_lines(lines):
    counter = Counter(lines)

    repeated = set()

    for line, count in counter.items():
        if (
            count >= MIN_REPEAT
            and len(line.strip()) > 0
            and len(line) < MAX_LINE_LEN
        ):
            repeated.add(line)

    return repeated


def remove_repeated(lines, repeated):
    cleaned = []
    removed = 0

    for line in lines:
        if line in repeated:
            removed += 1
            continue
        cleaned.append(line)

    return cleaned, removed


def remove_duplicate_metadata(text: str) -> str:
    parts = text.split("## Metadados")

    if len(parts) <= 2:
        return text

    # mantém primeira ocorrência
    return parts[0] + "## Metadados" + parts[1]


def clean_lines(text: str):
    lines = text.split("\n")

    # strip seguro
    stripped = [l.strip() for l in lines]

    # detectar repetição
    repeated = detect_repeated_lines(stripped)

    # remover repetidos
    cleaned, removed = remove_repeated(stripped, repeated)

    return cleaned, {
        "lines_input": len(lines),
        "lines_after_strip": len(stripped),
        "lines_removed_repetition": removed,
        "repeated_candidates": len(repeated),
    }


def rebuild_text(lines):
    return "\n".join(lines)


# ========================
# PROCESSAMENTO
# ========================

def process_file(raw_path: Path):
    clean_path = raw_path.with_name(raw_path.name.replace(".raw.md", ".clean.md"))
    meta_path = raw_path.with_name(raw_path.name.replace(".raw.md", ".clean.meta.json"))

    print(f"[INFO] Processing: {raw_path.name}")

    raw = raw_path.read_text(encoding="utf-8", errors="ignore")

    # step 1
    normalized = normalize_text(raw)

    # step 1.1
    deduped = remove_duplicate_metadata(normalized)

    # step 2
    lines, meta_lines = clean_lines(deduped)

    # step 3
    final_text = rebuild_text(lines)

    # write
    clean_path.write_text(final_text, encoding="utf-8")

    meta = {
        "source": raw_path.name,
        "output": clean_path.name,
        "chars_raw": len(raw),
        "chars_clean": len(final_text),
        "timestamp": datetime.now().isoformat(),
        **meta_lines,
    }

    meta_path.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"[OK] Clean generated: {clean_path.name}")


# ========================
# RUNNER
# ========================

def run():
    total = 0

    for projeto in KB_ROOT.iterdir():
        rag_md = projeto / "99_RAG" / "01_MD"

        if not rag_md.exists():
            continue

        for raw_file in rag_md.glob("*.raw.md"):
            process_file(raw_file)
            total += 1

    print(f"[DONE] Files processed: {total}")


if __name__ == "__main__":
    run()

