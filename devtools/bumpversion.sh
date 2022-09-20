# Script to increment Helm chart version
#
# Usage:
# $ bumpversion.sh VERSION
#
# Example:
# $ bumpversion.sh 2.14.0

function print_error() {
  echo -e "\033[31mERROR:\033[0m $1"
}

function print_info() {
  echo -e "\033[32mINFO:\033[0m  $1"
}

VERSION="$1"

git fetch origin > /dev/null
# get current branch
BRANCH=$(git branch | grep "^\*" | cut -d " " -f 2)
print_info "Current branch is '$BRANCH'."

if [ "$BRANCH" != "release/$VERSION" ]; then
  print_error "You should be on a release branch."
  print_error "Please start a new branch ``release/$VERSION``."
  exit 1
fi

print_info "Incrementing crate-operator Helm Chart to version '$VERSION'"
# Chart version
sed -i "s/^version:.*$/version: $VERSION/" deploy/charts/crate-operator/Chart.yaml
# App version
sed -i "s/^appVersion:.*$/appVersion: \"$VERSION\"/" deploy/charts/crate-operator/Chart.yaml
# Dependency version
sed -i "s/^  version:.*$/  version: $VERSION/" deploy/charts/crate-operator/Chart.yaml


print_info "Incrementing crate-operator-crds Helm Chart to version '$VERSION'"
# Chart version
sed -i "s/^version:.*$/version: $VERSION/" deploy/charts/crate-operator-crds/Chart.yaml
# App version
sed -i "s/^appVersion:.*$/appVersion: \"$VERSION\"/" deploy/charts/crate-operator-crds/Chart.yaml


print_info "Done. ✨"
