# CrateDB Kubernetes Operator
#
# Licensed to Crate.IO GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

FROM python:3.12-slim AS builder

WORKDIR /wheels

RUN pip install --no-cache-dir --upgrade pip wheel && \
    pip wheel --no-cache-dir bottle==0.13.4 waitress==3.0.2

FROM python:3.12-slim

LABEL license="Apache License 2.0" \
    maintainer="Crate.IO GmbH <office@crate.io>" \
    name="CrateDB Control Sidecar" \
    repository="crate/crate-operator"

WORKDIR /app

RUN useradd -U -M crate-control && \
    apt-get update && \
    apt-get upgrade -y && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /wheels /wheels

RUN pip install --no-cache-dir --no-compile /wheels/* && rm -rf /wheels

COPY main.py /app/

USER crate-control

EXPOSE 5050

CMD ["python", "-u", "main.py"]
