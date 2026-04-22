from pathlib import Path
from datetime import datetime
import shutil

import fitz
import pytesseract
from pdf2image import convert_from_path

from prefect import flow, get_run_logger


QUEUE = Path("/var/tmp/kb_rag_queue")
INBOX = QUEUE / "10_inbox"
WORK = QUEUE / "20_work"
OUTBOX = QUEUE / "30_outbox"
FAILED = QUEUE / "40_failed"
LOGS = QUEUE / "99_logs"

KB = Path("/srv/data/kb_projects")

MIN_TEXT_LEN = 200
PAGES_PER_BATCH = 10

def inferir_projeto(nome: str) -> str:
    return nome.split("__", 1)[0]

def now_ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def escrever_log(msg: str):
    LOGS.mkdir(parents=True, exist_ok=True)
    f = LOGS / f"kb_rag_v3_1_{datetime.now().strftime('%Y-%m-%d')}.log"
    with f.open("a", encoding="utf-8") as log:
        log.write(f"{datetime.now().isoformat()} | {msg}\n")


def extrair_texto_nativo(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    textos = []
    for page in doc:
        textos.append(page.get_text())
    doc.close()
    return "\n\n".join(textos).strip()


def extrair_texto_ocr_batch(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    total_paginas = len(doc)
    doc.close()

    escrever_log(f"OCR batch iniciado: {total_paginas} páginas")

    texto_final = []

    for inicio in range(1, total_paginas + 1, PAGES_PER_BATCH):
        fim = min(inicio + PAGES_PER_BATCH - 1, total_paginas)

        escrever_log(f"OCR páginas {inicio}–{fim}")

        imagens = convert_from_path(
            str(pdf_path),
            dpi=150,
            first_page=inicio,
            last_page=fim
        )

        for i, img in enumerate(imagens, start=inicio):
            escrever_log(f"OCR página {i}")
            texto = pytesseract.image_to_string(img, lang="por+eng")
            texto_final.append(f"\n\n--- Página {i} ---\n\n{texto.strip()}")

    escrever_log("OCR batch finalizado")

    return "\n".join(texto_final).strip()


def salvar_md(arq, projeto, origem, metodo, texto):
    ts = now_ts()
    md = KB / projeto / "99_RAG" / "01_MD" / f"{arq.stem}.{ts}.raw.md"
    md.parent.mkdir(parents=True, exist_ok=True)

    with md.open("w", encoding="utf-8") as f:
        f.write(f"# {arq.name}\n\n")
        f.write("## Metadados\n\n")
        f.write(f"- projeto: {projeto}\n")
        f.write(f"- metodo: {metodo}\n")
        f.write(f"- extraido_em: {datetime.now().isoformat()}\n\n")
        f.write("## Conteúdo\n\n")
        f.write(texto)

    return md


@flow(name="kb-rag-v3.1")
def kb_rag_v3_1():
    logger = get_run_logger()

    arquivos = list(INBOX.glob("*.pdf"))

    if not arquivos:
        logger.info("Inbox vazio")
        return

    for arq in arquivos:
        try:
            projeto = inferir_projeto(arq.name)

            work = WORK / arq.name
            WORK.mkdir(parents=True, exist_ok=True)
            shutil.copy2(arq, work)

            texto = extrair_texto_nativo(work)
            metodo = "native"

            if len(texto) < MIN_TEXT_LEN:
                logger.info("Fallback OCR")
                texto = extrair_texto_ocr_batch(work)
                metodo = "ocr"

            if len(texto) < MIN_TEXT_LEN:
                raise ValueError("Texto insuficiente")

            md = salvar_md(arq, projeto, work, metodo, texto)

            OUTBOX.mkdir(parents=True, exist_ok=True)
            (OUTBOX / f"{arq.stem}.ok").write_text("ok")

            logger.info(f"OK: {md}")

            work.unlink()

        except Exception as e:
            logger.error(str(e))

if __name__ == "__main__":
    kb_rag_v3_1()