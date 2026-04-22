from pathlib import Path
import re
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

        # leve bônus se termo aparece no início
        if text_n.find(t) != -1 and text_n.find(t) < 200:
            score += 1

    return score

# ========================
# CONTEXTO
# ========================

def snippet(text: str, terms, window=200):
    text_n = normalize(text)

    for t in terms:
        idx = text_n.find(t)
        if idx != -1:
            start = max(0, idx - window)
            end = min(len(text), idx + window)
            return text[start:end].replace("\n", " ")

    return text[:400].replace("\n", " ")

# ========================
# BUSCA
# ========================

def search(query: str, top_k=5):
    terms = tokenize(query)

    results = []

    for projeto in CHUNKS_ROOT.iterdir():
        chunk_dir = projeto / "99_RAG" / "02_Chunks"

        if not chunk_dir.exists():
            continue

        for file in chunk_dir.glob("*.md"):
            text = file.read_text(encoding="utf-8", errors="ignore")

            s = score_chunk(text, terms)

            if s > 0:
                results.append({
                    "file": str(file),
                    "score": s,
                    "snippet": snippet(text, terms)
                })

    results.sort(key=lambda x: x["score"], reverse=True)

    return results[:top_k]

# ========================
# CLI
# ========================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python kb_rag_v6_search.py 'sua pergunta'")
        exit()

    query = " ".join(sys.argv[1:])

    results = search(query)

    for r in results:
        print("\n---")
        print(f"[score={r['score']}] {r['file']}")
        print(r["snippet"])
