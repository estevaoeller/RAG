from pathlib import Path
from datetime import datetime
from prefect import flow, get_run_logger, task


PROJECT = "COMPESA"

BASE = Path(f"/srv/data/kb_projects/{PROJECT}")
INBOX = BASE / "_inbox"
WORKING = BASE / "03_Working"
MD_DIR = BASE / "99_RAG" / "01_MD"


@task
def listar():
    return [p for p in INBOX.iterdir() if p.is_file()]


@task
def gerar_md(arquivo: Path) -> Path:
    destino = MD_DIR / f"{arquivo.stem}.md"

    conteudo = f"""# {arquivo.name}

## Metadata

- caminho: {arquivo}
- tamanho: {arquivo.stat().st_size}
- processado_em: {datetime.now().isoformat()}

## Conteúdo

(placeholder)
"""

    with destino.open("w", encoding="utf-8") as f:
        f.write(conteudo)

    return destino


@task
def mover_para_working(arquivo: Path) -> Path:
    destino = WORKING / arquivo.name
    arquivo.rename(destino)
    return destino


@flow(name="project-md-ingest")
def project_md_ingest():
    logger = get_run_logger()

    MD_DIR.mkdir(parents=True, exist_ok=True)
    WORKING.mkdir(parents=True, exist_ok=True)

    arquivos = listar()

    if not arquivos:
        logger.info("Inbox vazio.")
        return

    for a in arquivos:
        try:
            md = gerar_md(a)
            mover_para_working(a)

            logger.info(f"OK: {a.name} -> {md}")

        except Exception as e:
            logger.error(f"Erro em {a.name}: {e}")


if __name__ == "__main__":
    project_md_ingest()
