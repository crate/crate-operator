#!/bin/bash
#
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
#
# ---
#
# Script to create and push a Git tag
#
# Usage:
# $ create_tag.sh VERSION
#
# Example:
# $ create_tag.sh 0.12

function print_error() {
  echo -e "\033[31mERROR:\033[0m $1"
}

function print_info() {
  echo -e "\033[32mINFO:\033[0m  $1"
}

VERSION="$1"

# check if everything is committed
CLEAN=$(git status -s)
if [ -n "$CLEAN" ]; then
  print_error "Working directory not clean."
  print_error "Please commit all changes first."
  exit 1
fi

git fetch origin > /dev/null
# get current branch
BRANCH=$(git branch | grep "^\*" | cut -d " " -f 2)
print_info "Current branch is '$BRANCH'."

if [ "$BRANCH" != "master" ]; then
  print_error "Cannot create release from branch '$BRANCH'."
  print_error "Must be 'master'."
  exit 1
fi

# check if local branch is origin branch
LOCAL_COMMIT=$(git show --format="%H" "$BRANCH")
ORIGIN_COMMIT=$(git show --format="%H" "origin/$BRANCH")

if [ "$LOCAL_COMMIT" != "$ORIGIN_COMMIT" ]; then
  print_error "Local branch '$BRANCH' is not up to date."
  print_error "Please push your changes first."
  exit 1
fi

# check if $VERSION is in head of CHANGES.rst
REV_NOTE=$(grep "$VERSION ([0-9-]\{10\})" CHANGES.rst)
if [ -z "$REV_NOTE" ]; then
  print_error "No section for release '$VERSION' found in CHANGES.rst"
  exit 1
fi

print_info "Creating tag '$VERSION'"
git tag -a "$VERSION" -m "Tag release for revision '$VERSION'"

# check if Python package has correct version
PKG_VERSION=$(python setup.py --version)
if [ "$VERSION" != "$PKG_VERSION" ] ; then
  print_error "Git version and package version do not match."
  print_error "$VERSION != $PKG_VERSION"
  git tag -d "$VERSION"
  exit 1
fi

git push --tags

COMMIT="$(git rev-parse --short HEAD)"
print_info ""
print_info "Version: $VERSION"
print_info "Commit : $COMMIT"
print_info "Done. ✨"
