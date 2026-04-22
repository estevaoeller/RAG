from prefect import flow, get_run_logger


@flow(name="teste-minimo")
def teste_minimo():
    logger = get_run_logger()
    logger.info("Prefect está funcionando no servidor.")
    print("Execução local do flow concluída.")


if __name__ == "__main__":
    teste_minimo()
