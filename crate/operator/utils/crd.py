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


def has_compute_changed(old_spec, new_spec) -> bool:
    """
    Compares the old and new cpu and memory configuration.

    :param old_spec: The old resource body.
    :param new_spec: The new resource body.
    """
    return (
        old_spec.get("resources", {}).get("limits", {}).get("cpu")
        != new_spec.get("resources", {}).get("limits", {}).get("cpu")
        or old_spec.get("resources", {}).get("requests", {}).get("cpu")
        != new_spec.get("resources", {}).get("requests", {}).get("cpu")
        or old_spec.get("resources", {}).get("limits", {}).get("memory")
        != new_spec.get("resources", {}).get("limits", {}).get("memory")
        or old_spec.get("resources", {}).get("requests", {}).get("memory")
        != new_spec.get("resources", {}).get("requests", {}).get("memory")
        or old_spec.get("nodepool") != new_spec.get("nodepool")
    )
