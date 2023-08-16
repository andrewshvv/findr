FROM python:3.9.17

# Set environment variables
ENV  PYTHONDONTWRITEBYTECODE=1 \
     PYTHONUNBUFFERED=1 \
     POETRY_VERSION=1.5.1 \
     POETRY_NO_INTERACTION=1 \
     POETRY_VIRTUALENVS_IN_PROJECT=1 \
     POETRY_VIRTUALENVS_CREATE=1 \
     POETRY_CACHE_DIR=/tmp/poetry_cache \
     VIRTUAL_ENV=/app/.venv \
     PATH="/app/.venv/bin:$PATH"

# Install poetry
RUN apt-get update && \
    apt-get install -y firefox-esr && \
    pip install --upgrade pip && \
    pip install "poetry==$POETRY_VERSION"

# Set working directory
WORKDIR ./app

# Project initialization
COPY poetry.lock pyproject.toml ./
RUN --mount=type=cache,target=$POETRY_CACHE_DIR poetry install --without dev --no-root

# Re-install crhoma hnswlib because for some reason it fails with illegal instractions
# as if it compiles for the wrong acrhitecture
# TODO: Need to revisit
RUN CHROMA_HNSWLIB_VERSION=$(poetry show -v | grep chroma-hnswlib | awk '{print $2}') &&  \
    poetry run pip uninstall chroma-hnswlib &&  \
    poetry run pip install chroma-hnswlib=="${CHROMA_HNSWLIB_VERSION}"


COPY src src

# Run the command to start your application
CMD ["python", "./src/main.py"]
#CMD ["/bin/bash"]