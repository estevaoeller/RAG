from pathlib import Path
from datetime import datetime
import json
import re

KB_ROOT = Path("/srv/data/kb_projects")

RE_CLAUSULA = re.compile(r"^\s*CL[ÁA]USULA\b", re.IGNORECASE)

MAX_CHARS = 4000

# ========================
# DETECTAR INÍCIO DO CORPO
# ========================

def find_body_start(lines):
    count = 0

    for i, line in enumerate(lines):
        if RE_CLAUSULA.match(line):
            count += 1
            if count == 2:
                return i

    return 0


# ========================
# SPLIT PRINCIPAL
# ========================

def split_clauses(lines):
    chunks = []
    current = []

    for line in lines:
        if RE_CLAUSULA.match(line):
            if current:
                chunks.append("\n".join(current).strip())
                current = []

        current.append(line)

    if current:
        chunks.append("\n".join(current).strip())

    return chunks


# ========================
# FILTRO DE QUALIDADE
# ========================

def is_valid_chunk(text):
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    if len(lines) <= 1:
        return False

    if len(text) < 100:
        return False

    return True


# ========================
# SPLIT DE TAMANHO
# ========================

def split_large_chunk(text):
    if len(text) <= MAX_CHARS:
        return [text]

    parts = []
    current = ""

    for line in text.split("\n"):
        if len(current) + len(line) < MAX_CHARS:
            current += line + "\n"
        else:
            parts.append(current.strip())
            current = line + "\n"

    if current:
        parts.append(current.strip())

    return parts


# ========================
# PROCESSAMENTO
# ========================

def process_file(md_path: Path):
    base = md_path.name.replace(".clean.v4_3.md", "")

    chunk_dir = md_path.parent.parent / "02_Chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)

    text = md_path.read_text(encoding="utf-8", errors="ignore")
    lines = text.split("\n")

    # 🔴 remover índice
    start = find_body_start(lines)
    lines = lines[start:]

    clause_chunks = split_clauses(lines)

    final_chunks = []

    for chunk in clause_chunks:
        if not is_valid_chunk(chunk):
            continue

        final_chunks.extend(split_large_chunk(chunk))

    chunk_files = []

    for i, chunk in enumerate(final_chunks, 1):
        fname = f"{base}.chunk_{i:04d}.md"
        fpath = chunk_dir / fname

        fpath.write_text(chunk, encoding="utf-8")

        chunk_files.append({
            "file": fname,
            "chars": len(chunk)
        })

    meta = {
        "source": md_path.name,
        "chunks": len(chunk_files),
        "generated_at": datetime.now().isoformat(),
        "files": chunk_files
    }

    meta_path = chunk_dir / f"{base}.chunks.json"
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"[OK] {md_path.name} → {len(chunk_files)} chunks")


# ========================
# RUNNER
# ========================

def run():
    for projeto in KB_ROOT.iterdir():
        rag_md = projeto / "99_RAG" / "01_MD"

        if not rag_md.exists():
            continue

        for file in rag_md.glob("*.clean.v4_3.md"):
            process_file(file)


if __name__ == "__main__":
    run()