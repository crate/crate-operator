# Build container
FROM python:3.8-slim AS build

RUN mkdir -pv /src

WORKDIR /src

RUN apt-get update && \
    apt-get install git -y

COPY . /src
RUN python -m pip install -U setuptools==65.5.1 && \
    python setup.py clean bdist_wheel


# Run container
FROM python:3.8-slim

LABEL license="Apache License 2.0" \
      maintainer="Crate.IO GmbH <office@crate.io>" \
      name="CrateDB Kubernetes Operator" \
      repository="crate/crate-operator"

WORKDIR /etc/cloud
RUN useradd -U -M crate-operator

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /src/dist /wheels

RUN pip install --no-cache-dir -U pip wheel setuptools==65.5.1 && \
    pip install --no-cache-dir /wheels/*.whl && \
    rm -rf /wheels && \
    ln -s "$(python -c "import pkgutil; main = pkgutil.get_loader('crate.operator.main'); print(main.path)")"

USER crate-operator

ENTRYPOINT ["kopf", "run", "--standalone", "-A"]
CMD ["main.py"]
