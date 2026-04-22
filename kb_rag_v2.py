from pathlib import Path
from datetime import datetime
import fitz  # pymupdf

from prefect import flow, get_run_logger


QUEUE = Path("/var/tmp/kb_rag_queue")
INBOX = QUEUE / "10_inbox"
WORK = QUEUE / "20_work"
OUTBOX = QUEUE / "30_outbox"

KB = Path("/srv/data/kb_projects")


def inferir_projeto(nome):
    return nome.split("__")[0]


def extrair_texto(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    textos = []

    for page in doc:
        textos.append(page.get_text())

    return "\n\n".join(textos)


@flow(name="kb-rag-v2")
def kb_rag_v2():
    logger = get_run_logger()

    arquivos = list(INBOX.glob("*"))

    if not arquivos:
        logger.info("Inbox vazio")
        return

    for arq in arquivos:
        try:
            projeto = inferir_projeto(arq.name)

            # work
            work_file = WORK / arq.name
            WORK.mkdir(exist_ok=True, parents=True)
            arq.replace(work_file)

            # extração
            texto = extrair_texto(work_file)

            # destino final
            md_path = KB / projeto / "99_RAG" / "01_MD" / f"{arq.stem}.md"
            md_path.parent.mkdir(parents=True, exist_ok=True)

            with open(md_path, "w", encoding="utf-8") as f:
                f.write(f"# {arq.name}\n\n")
                f.write(f"## extraído em {datetime.now()}\n\n")
                f.write(texto)

            logger.info(f"OK: {md_path}")

            # outbox
            OUTBOX.mkdir(exist_ok=True, parents=True)
            (OUTBOX / f"{arq.stem}.ok").write_text("ok")

            # limpar work
            work_file.unlink()

        except Exception as e:
            logger.error(f"Erro: {arq.name} → {e}")

if __name__ == "__main__":
    kb_rag_v2()