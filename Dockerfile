FROM jupyter/scipy-notebook

USER root

RUN apt update && apt install -y graphviz && apt clean
RUN pip install poetry requests graphviz sacred imbalanced-learn

COPY pyproject.toml .
COPY poetry.lock .
COPY pyfxlib ./pyfxlib

USER $NB_UID

RUN poetry install --no-dev

ENTRYPOINT ["poetry", "run"]
CMD ["python", "-m", "pyfxlib.env"]
