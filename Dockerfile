ARG SETUPTOOLS_VERSION=80.10.1

# Build container
FROM python:3.12-slim AS build

ARG SETUPTOOLS_VERSION

RUN mkdir -pv /src

WORKDIR /src

RUN apt-get update && \
    apt-get install git -y

COPY . /src
RUN python -m pip install -U setuptools==${SETUPTOOLS_VERSION} && \
    python setup.py clean bdist_wheel


# Run container
FROM python:3.12-slim

LABEL license="Apache License 2.0" \
    maintainer="Crate.IO GmbH <office@crate.io>" \
    name="CrateDB Kubernetes Operator" \
    repository="crate/crate-operator"

ARG SETUPTOOLS_VERSION

WORKDIR /etc/cloud
RUN useradd -U -M crate-operator

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /src/dist /wheels

RUN pip install --no-cache-dir -U pip wheel setuptools==${SETUPTOOLS_VERSION} && \
    pip install --no-cache-dir /wheels/*.whl && \
    rm -rf /wheels && \
    ln -s "$(python -c "import pkgutil; main = pkgutil.get_loader('crate.operator.main'); print(main.path)")"

USER crate-operator

ENTRYPOINT ["kopf", "run", "--standalone", "-A"]
CMD ["main.py"]
