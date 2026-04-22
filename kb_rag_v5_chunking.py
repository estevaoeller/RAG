from pathlib import Path
from datetime import datetime
import json
import re

KB_ROOT = Path("/srv/data/kb_projects")

RE_CLAUSULA = re.compile(r"^\s*CL[ÁA]USULA\b", re.IGNORECASE)

MAX_CHARS = 4000
MIN_CHARS = 1500

# ========================
# DETECTAR INÍCIO DO CORPO
# ========================

def find_body_start(lines):
    re_cap1 = re.compile(r"^\s*CAP[IÍ]TULO\s+[I1]+\b", re.IGNORECASE)
    re_cl1 = re.compile(r"^\s*CL[ÁA]USULA\s+1\b", re.IGNORECASE)

    cap_hits = []
    cl_hits = []

    for i, line in enumerate(lines):
        stripped = line.strip()
        if re_cap1.match(stripped):
            cap_hits.append(i)
        if re_cl1.match(stripped):
            cl_hits.append(i)

    if len(cl_hits) >= 2:
        return cl_hits[1]

    if len(cap_hits) >= 2:
        return cap_hits[1]

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

    body = "\n".join(lines[1:]).strip()

    if len(body) < 80:
        return False

    return True

# ========================
# MERGE DE CHUNKS PEQUENOS
# ========================

def merge_small_chunks(chunks):
    if not chunks:
        return []

    merged = []
    buffer = ""

    for chunk in chunks:
        chunk = chunk.strip()

        if not buffer:
            buffer = chunk
            continue

        # se buffer ainda é pequeno, junta com o próximo
        if len(buffer) < MIN_CHARS:
            candidate = buffer + "\n\n" + chunk

            # se não estourar demais, mantém unido
            if len(candidate) <= MAX_CHARS:
                buffer = candidate
            else:
                merged.append(buffer.strip())
                buffer = chunk
        else:
            merged.append(buffer.strip())
            buffer = chunk

    if buffer:
        merged.append(buffer.strip())

    return merged

# ========================
# SPLIT DE TAMANHO
# ========================

def split_large_chunk(text):
    if len(text) <= MAX_CHARS:
        return [text]

    parts = []
    current = ""

    for line in text.split("\n"):
        if len(current) + len(line) + 1 <= MAX_CHARS:
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

    # cortar índice / começar no corpo real
    start = find_body_start(lines)
    lines = lines[start:]

    # split inicial por cláusula
    clause_chunks = split_clauses(lines)

    # remover chunks ruins
    valid_chunks = [c for c in clause_chunks if is_valid_chunk(c)]

    # juntar chunks pequenos
    merged_chunks = merge_small_chunks(valid_chunks)

    # dividir apenas os grandes demais
    final_chunks = []
    for chunk in merged_chunks:
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
        "min_chars_target": MIN_CHARS,
        "max_chars_target": MAX_CHARS,
        "files": chunk_files
    }

    meta_path = chunk_dir / f"{base}.chunks.json"
    meta_path.write_text(
        json.dumps(meta, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

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