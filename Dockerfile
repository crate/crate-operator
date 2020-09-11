# Build container
FROM python:3.8-slim AS build

RUN mkdir -pv /src

WORKDIR /src

RUN apt-get update && \
    apt-get install git -y

COPY . /src
RUN python setup.py clean bdist_wheel


# Run container
FROM python:3.8-slim

LABEL license="AGPLv3" \
      maintainer="Crate.io AT GmbH <office@crate.io>" \
      name="CrateDB Kubernetes Operator" \
      repository="crate/crate-operator"

WORKDIR /etc/cloud
RUN useradd -U -M crate-operator

COPY --from=build /src/dist /wheels

RUN pip install --no-cache-dir -U pip wheel && \
    pip install --no-cache-dir /wheels/*.whl && \
    rm -rf /wheels && \
    ln -s "$(python -c "import pkgutil; main = pkgutil.get_loader('crate.operator.main'); print(main.path)")"

USER crate-operator

ENTRYPOINT ["kopf", "run", "--standalone"]
CMD ["main.py"]
