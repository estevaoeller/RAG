from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path

from prefect import flow, get_run_logger, task


QUEUE_BASE = Path("/var/tmp/kb_rag_queue")
INBOX = QUEUE_BASE / "10_inbox"
WORK = QUEUE_BASE / "20_work"
OUTBOX = QUEUE_BASE / "30_outbox"
FAILED = QUEUE_BASE / "40_failed"
LOGS = QUEUE_BASE / "99_logs"

KB_BASE = Path("/srv/data/kb_projects")


def inferir_projeto(nome_arquivo: str) -> str:
    if "__" not in nome_arquivo:
        raise ValueError(f"Arquivo sem padrão '<PROJETO>__...': {nome_arquivo}")
    projeto = nome_arquivo.split("__", 1)[0].strip()
    if not projeto:
        raise ValueError(f"Projeto vazio no nome do arquivo: {nome_arquivo}")
    return projeto


def caminho_rag_md(projeto: str, nome_arquivo: str) -> Path:
    return KB_BASE / projeto / "99_RAG" / "01_MD" / f"{Path(nome_arquivo).stem}.md"


def escrever_log(msg: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    arq = LOGS / f"kb_rag_v1_{datetime.now().strftime('%Y-%m-%d')}.log"
    with arq.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} | {msg}\n")


@task
def garantir_filas() -> None:
    for p in [INBOX, WORK, OUTBOX, FAILED, LOGS]:
        p.mkdir(parents=True, exist_ok=True)


@task
def listar_inbox() -> list[Path]:
    return sorted([p for p in INBOX.iterdir() if p.is_file()])


@task
def copiar_para_work(arquivo: Path) -> Path:
    destino = WORK / arquivo.name
    shutil.copy2(arquivo, destino)
    return destino


@task
def gerar_md_placeholder(arquivo_work: Path, projeto: str) -> Path:
    destino = caminho_rag_md(projeto, arquivo_work.name)
    destino.parent.mkdir(parents=True, exist_ok=True)

    conteudo = f"""# {arquivo_work.name}

## Metadados

- projeto: {projeto}
- arquivo_original_fila: {arquivo_work.name}
- origem_operacional: {arquivo_work}
- gerado_em: {datetime.now().isoformat()}
- status: placeholder_v1

## Observação

Este arquivo foi criado pela v1 do pipeline.
Ainda não houve extração de texto do PDF.
"""

    with destino.open("w", encoding="utf-8") as f:
        f.write(conteudo)

    return destino


@task
def gerar_manifesto_outbox(arquivo_work: Path, projeto: str, md_path: Path) -> Path:
    destino = OUTBOX / f"{arquivo_work.stem}.json"
    payload = {
        "projeto": projeto,
        "arquivo_fila": arquivo_work.name,
        "arquivo_work": str(arquivo_work),
        "md_destino": str(md_path),
        "processado_em": datetime.now().isoformat(),
        "status": "ok",
    }
    with destino.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return destino


@task
def mover_para_failed(arquivo: Path, erro: str) -> Path:
    destino = FAILED / arquivo.name
    if arquivo.exists():
        shutil.move(str(arquivo), str(destino))

    erro_path = FAILED / f"{arquivo.stem}.error.txt"
    with erro_path.open("w", encoding="utf-8") as f:
        f.write(erro)

    return destino


@task
def limpar_work(arquivo_work: Path) -> None:
    if arquivo_work.exists():
        arquivo_work.unlink()


@flow(name="kb-rag-v1")
def kb_rag_v1():
    logger = get_run_logger()

    garantir_filas()
    arquivos = listar_inbox()

    if not arquivos:
        logger.info("Nenhum arquivo em 10_inbox.")
        escrever_log("Nenhum arquivo em 10_inbox.")
        return

    logger.info(f"{len(arquivos)} arquivo(s) encontrado(s) em 10_inbox.")
    escrever_log(f"{len(arquivos)} arquivo(s) encontrado(s) em 10_inbox.")

    for arquivo in arquivos:
        arquivo_work = None
        try:
            projeto = inferir_projeto(arquivo.name)
            logger.info(f"{arquivo.name} -> projeto={projeto}")
            escrever_log(f"{arquivo.name} -> projeto={projeto}")

            arquivo_work = copiar_para_work(arquivo)
            logger.info(f"Copiado para work: {arquivo_work}")
            escrever_log(f"Copiado para work: {arquivo_work}")

            md_path = gerar_md_placeholder(arquivo_work, projeto)
            logger.info(f"MD gerado: {md_path}")
            escrever_log(f"MD gerado: {md_path}")

            manifesto = gerar_manifesto_outbox(arquivo_work, projeto, md_path)
            logger.info(f"Manifesto operacional gerado: {manifesto}")
            escrever_log(f"Manifesto operacional gerado: {manifesto}")

            limpar_work(arquivo_work)
            logger.info(f"Work limpo: {arquivo_work.name}")
            escrever_log(f"Work limpo: {arquivo_work.name}")

        except Exception as e:
            erro = f"Erro ao processar {arquivo.name}: {e}"
            logger.error(erro)
            escrever_log(erro)

            try:
                alvo = arquivo_work if arquivo_work and arquivo_work.exists() else arquivo
                failed_path = mover_para_failed(alvo, erro)
                logger.info(f"Movido para failed: {failed_path}")
                escrever_log(f"Movido para failed: {failed_path}")
            except Exception as e2:
                erro2 = f"Falha adicional ao mover para failed: {e2}"
                logger.error(erro2)
                escrever_log(erro2)


if __name__ == "__main__":
    kb_rag_v1()
