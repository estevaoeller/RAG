from pathlib import Path
from datetime import datetime
import shutil

import fitz  # pymupdf
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


def inferir_projeto(nome: str) -> str:
    if "__" not in nome:
        raise ValueError(f"Nome fora do padrão <PROJETO>__arquivo: {nome}")
    projeto = nome.split("__", 1)[0].strip()
    if not projeto:
        raise ValueError(f"Projeto vazio no nome do arquivo: {nome}")
    return projeto


def escrever_log(msg: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    log_file = LOGS / f"kb_rag_v3_{datetime.now().strftime('%Y-%m-%d')}.log"
    with log_file.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} | {msg}\n")


def extrair_texto_nativo(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    textos = []
    try:
        for page in doc:
            textos.append(page.get_text())
    finally:
        doc.close()
    return "\n\n".join(textos).strip()


def extrair_texto_ocr(pdf_path: Path) -> str:
    imagens = convert_from_path(str(pdf_path), dpi=300)
    textos = []

    for i, imagem in enumerate(imagens, start=1):
        texto = pytesseract.image_to_string(imagem, lang="por")
        textos.append(f"\n\n--- Página {i} ---\n\n{texto.strip()}")

    return "\n".join(textos).strip()


def salvar_md(
    arq_nome: str,
    projeto: str,
    origem_operacional: Path,
    metodo: str,
    texto: str,
) -> Path:
    md_path = KB / projeto / "99_RAG" / "01_MD" / f"{Path(arq_nome).stem}.md"
    md_path.parent.mkdir(parents=True, exist_ok=True)

    with md_path.open("w", encoding="utf-8") as f:
        f.write(f"# {arq_nome}\n\n")
        f.write("## Metadados\n\n")
        f.write(f"- projeto: {projeto}\n")
        f.write(f"- arquivo_original_fila: {arq_nome}\n")
        f.write(f"- origem_operacional: {origem_operacional}\n")
        f.write(f"- extraido_em: {datetime.now().isoformat()}\n")
        f.write(f"- metodo_extracao: {metodo}\n\n")
        f.write("## Conteúdo extraído\n\n")
        f.write(texto)

    return md_path


def marcar_ok(nome_base: str, metodo: str, texto_len: int) -> Path:
    OUTBOX.mkdir(parents=True, exist_ok=True)
    ok_path = OUTBOX / f"{nome_base}.ok"
    ok_path.write_text(
        f"status=ok\nmetodo={metodo}\ntexto_len={texto_len}\n",
        encoding="utf-8",
    )
    return ok_path


def registrar_failed(arq: Path, erro: str) -> None:
    FAILED.mkdir(parents=True, exist_ok=True)

    failed_copy = FAILED / arq.name
    if arq.exists() and not failed_copy.exists():
        shutil.copy2(arq, failed_copy)

    erro_txt = FAILED / f"{arq.stem}.error.txt"
    erro_txt.write_text(
        f"{datetime.now().isoformat()} | {erro}\n",
        encoding="utf-8",
    )


@flow(name="kb-rag-v3")
def kb_rag_v3():
    logger = get_run_logger()

    for pasta in [INBOX, WORK, OUTBOX, FAILED, LOGS]:
        pasta.mkdir(parents=True, exist_ok=True)

    arquivos = [p for p in INBOX.iterdir() if p.is_file()]

    if not arquivos:
        logger.info("Nenhum arquivo em 10_inbox.")
        escrever_log("Nenhum arquivo em 10_inbox.")
        return

    logger.info(f"{len(arquivos)} arquivo(s) encontrado(s) em 10_inbox.")
    escrever_log(f"{len(arquivos)} arquivo(s) encontrado(s) em 10_inbox.")

    for arq in arquivos:
        work_file = WORK / arq.name

        try:
            projeto = inferir_projeto(arq.name)
            logger.info(f"{arq.name} -> projeto={projeto}")
            escrever_log(f"{arq.name} -> projeto={projeto}")

            shutil.copy2(arq, work_file)
            logger.info(f"Copiado para work: {work_file}")
            escrever_log(f"Copiado para work: {work_file}")

            texto = extrair_texto_nativo(work_file)
            metodo = "native"

            if len(texto.strip()) < MIN_TEXT_LEN:
                logger.info(f"Texto nativo insuficiente para {arq.name}. Fallback OCR.")
                escrever_log(f"Texto nativo insuficiente para {arq.name}. Fallback OCR.")
                texto = extrair_texto_ocr(work_file)
                metodo = "ocr"

            if len(texto.strip()) < MIN_TEXT_LEN:
                raise ValueError(
                    f"Texto extraído insuficiente mesmo após OCR: {len(texto.strip())} caracteres"
                )

            md_path = salvar_md(
                arq_nome=arq.name,
                projeto=projeto,
                origem_operacional=work_file,
                metodo=metodo,
                texto=texto,
            )

            logger.info(f"MD gerado: {md_path}")
            escrever_log(f"MD gerado: {md_path}")

            ok_path = marcar_ok(arq.stem, metodo, len(texto))
            logger.info(f"Marcador de sucesso gerado: {ok_path}")
            escrever_log(f"Marcador de sucesso gerado: {ok_path}")

            if work_file.exists():
                work_file.unlink()

            logger.info(f"Work limpo: {work_file.name}")
            escrever_log(f"Work limpo: {work_file.name}")

        except Exception as e:
            erro = f"Erro ao processar {arq.name}: {e}"
            logger.error(erro)
            escrever_log(erro)

            try:
                registrar_failed(arq, erro)
                logger.info(f"Falha registrada em {FAILED}")
                escrever_log(f"Falha registrada em {FAILED}")
            except Exception as e2:
                erro2 = f"Falha adicional ao registrar erro de {arq.name}: {e2}"
                logger.error(erro2)
                escrever_log(erro2)

            if work_file.exists():
                try:
                    work_file.unlink()
                except Exception:
                    pass


if __name__ == "__main__":
    kb_rag_v3()
