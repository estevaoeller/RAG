from pathlib import Path
from datetime import datetime
import json
import re

# ========================
# CONFIG
# ========================

KB_ROOT = Path("/srv/data/kb_projects")

# padrões estruturais
RE_CAPITULO = re.compile(r"^\s*CAP[IÍ]TULO\s+[IVXLCDM0-9]+\b", re.IGNORECASE)
RE_CLAUSULA = re.compile(r"^\s*CL[ÁA]USULA\s+\d+\b", re.IGNORECASE)
RE_ANEXO = re.compile(r"^\s*ANEXO\s+[A-Z0-9IVXLCDM]+\b", re.IGNORECASE)
RE_CONSIDERANDOS = re.compile(r"^\s*CONSIDERANDOS\s*$", re.IGNORECASE)

# ========================
# DETECÇÃO
# ========================

def is_structural_marker(line: str) -> bool:
    line = line.strip()

    return (
        RE_CAPITULO.match(line)
        or RE_CLAUSULA.match(line)
        or RE_ANEXO.match(line)
        or RE_CONSIDERANDOS.match(line)
    )

# ========================
# AJUSTE DE ESTRUTURA
# ========================

def normalize_structural_spacing(lines):
    output = []

    for i, line in enumerate(lines):
        stripped = line.strip()

        if is_structural_marker(stripped):
            # garante linha em branco antes
            if output and output[-1] != "":
                output.append("")

            output.append(stripped)

            # garante linha em branco depois
            output.append("")
        else:
            output.append(line)

    return output

# ========================
# MERGE DE PARÁGRAFOS
# ========================

def should_merge(prev, curr):
    if not prev or not curr:
        return False

    prev = prev.strip()
    curr = curr.strip()

    # não juntar se for marcador estrutural
    if is_structural_marker(curr):
        return False

    # não juntar marcador de página
    if curr.startswith("--- Página"):
        return False

    # se linha anterior termina sem pontuação forte
    if not re.search(r"[.!?:;]$", prev):
        return True

    return False


def merge_broken_paragraphs(lines):
    merged = []

    for line in lines:
        if merged and should_merge(merged[-1], line):
            merged[-1] += " " + line.strip()
        else:
            merged.append(line)

    return merged

# ========================
# PROCESSAMENTO
# ========================

def process_file(clean_v42_path: Path):
    name = clean_v42_path.name

    if ".clean.v4_2.md" not in name:
        print(f"[SKIP] Not v4_2 file: {name}")
        return

    base = name.replace(".clean.v4_2.md", "")

    clean_v43_path = clean_v42_path.with_name(f"{base}.clean.v4_3.md")
    meta_path = clean_v42_path.with_name(f"{base}.clean.v4_3.meta.json")

    print(f"[INFO] Processing: {name}")

    raw = clean_v42_path.read_text(encoding="utf-8", errors="ignore")

    lines = raw.split("\n")

    # step 1: reforço estrutural
    structured = normalize_structural_spacing(lines)

    # step 2: merge leve
    merged = merge_broken_paragraphs(structured)

    final_text = "\n".join(merged)

    # write
    clean_v43_path.write_text(final_text, encoding="utf-8")

    meta = {
        "source": clean_v42_path.name,
        "output": clean_v43_path.name,
        "timestamp": datetime.now().isoformat(),
        "lines_input": len(lines),
        "lines_output": len(merged),
    }

    meta_path.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"[OK] Generated: {clean_v43_path.name}")

# ========================
# RUNNER
# ========================

def run():
    total = 0

    for projeto in KB_ROOT.iterdir():
        rag_md = projeto / "99_RAG" / "01_MD"

        if not rag_md.exists():
            continue

        for file in rag_md.glob("*.clean.v4_2.md"):
            process_file(file)
            total += 1

    print(f"[DONE] Files processed: {total}")

if __name__ == "__main__":
    run()
