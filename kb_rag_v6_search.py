from pathlib import Path
import re
import sys
import unicodedata

CHUNKS_ROOT = Path("/srv/data/kb_projects")

# ========================
# NORMALIZAÇÃO
# ========================

def normalize(text: str) -> str:
    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text

def tokenize(query: str):
    return [t for t in re.split(r"\W+", normalize(query)) if t]

# ========================
# SCORING
# ========================

def score_chunk(text: str, terms):
    text_n = normalize(text)
    score = 0

    for t in terms:
        count = text_n.count(t)
        score += count

        pos = text_n.find(t)
        if pos != -1 and pos < 200:
            score += 1

    return score

# ========================
# EXTRAÇÃO DE METADADOS
# ========================

def extract_chunk_number(filename: str) -> str:
    m = re.search(r"\.chunk_(\d{4})\.md$", filename)
    return m.group(1) if m else "----"

def extract_project_name(path: Path) -> str:
    # /srv/data/kb_projects/<PROJETO>/99_RAG/02_Chunks/arquivo.md
    try:
        return path.parts[path.parts.index("kb_projects") + 1]
    except Exception:
        return "UNKNOWN"

# ========================
# SNIPPET
# ========================

def snippet(text: str, terms, window=220):
    text_n = normalize(text)

    for t in terms:
        idx = text_n.find(t)
        if idx != -1:
            start = max(0, idx - window)
            end = min(len(text), idx + window)
            return compact_whitespace(text[start:end])

    return compact_whitespace(text[:400])

def compact_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

def highlight_terms(text: str, terms):
    result = text

    # destacar termos maiores primeiro
    ordered = sorted(set(terms), key=len, reverse=True)

    for term in ordered:
        if not term:
            continue

        pattern = re.compile(re.escape(term), re.IGNORECASE)

        def repl(match):
            return f"[[{match.group(0)}]]"

        result = pattern.sub(repl, result)

    return result

# ========================
# BUSCA
# ========================

def search(query: str, top_k=5, project=None):
    terms = tokenize(query)
    results = []

    for projeto_dir in CHUNKS_ROOT.iterdir():
        if not projeto_dir.is_dir():
            continue

        projeto = projeto_dir.name

        if project and normalize(project) != normalize(projeto):
            continue

        chunk_dir = projeto_dir / "99_RAG" / "02_Chunks"
        if not chunk_dir.exists():
            continue

        for file in chunk_dir.glob("*.md"):
            text = file.read_text(encoding="utf-8", errors="ignore")
            s = score_chunk(text, terms)

            if s > 0:
                snip = snippet(text, terms)
                snip = highlight_terms(snip, terms)

                results.append({
                    "project": projeto,
                    "file": file.name,
                    "chunk": extract_chunk_number(file.name),
                    "score": s,
                    "snippet": snip,
                })

    results.sort(key=lambda x: (x["score"], x["file"]), reverse=True)
    return results[:top_k]

# ========================
# CLI
# ========================

def parse_args(argv):
    project = None
    top_k = 5
    query_parts = []

    i = 0
    while i < len(argv):
        arg = argv[i]

        if arg == "--project" and i + 1 < len(argv):
            project = argv[i + 1]
            i += 2
            continue

        if arg == "--top" and i + 1 < len(argv):
            try:
                top_k = int(argv[i + 1])
            except ValueError:
                pass
            i += 2
            continue

        query_parts.append(arg)
        i += 1

    query = " ".join(query_parts).strip()
    return query, top_k, project

def main():
    query, top_k, project = parse_args(sys.argv[1:])

    if not query:
        print("Uso:")
        print("  python kb_rag_v6_1_search.py \"equilibrio economico financeiro\"")
        print("  python kb_rag_v6_1_search.py --project COMPESA --top 8 \"contraprestacao cbos\"")
        sys.exit(1)

    results = search(query=query, top_k=top_k, project=project)

    if not results:
        print("Nenhum resultado encontrado.")
        return

    print(f"Consulta: {query}")
    if project:
        print(f"Projeto: {project}")
    print(f"Resultados: {len(results)}")

    for r in results:
        print("\n---")
        print(f"Projeto: {r['project']}")
        print(f"Arquivo:  {r['file']}")
        print(f"Chunk:    {r['chunk']}")
        print(f"Score:    {r['score']}")
        print(f"Trecho:   {r['snippet']}")

if __name__ == "__main__":
    main()