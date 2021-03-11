#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

ENVIRONMENT=${1:-dev}

if [[ "$ENVIRONMENT" == "dev" ]]; then
  gfortran -Wall -Wextra -g -Wpedantic -std=f2003 -Wimplicit-interface src/*Component.f03 src/*CLI.f03 src/Main.f03 -o test/sc
elif [[ "$ENVIRONMENT" == "cli" ]]; then
  mkdir -p target/cli
  gfortran -Wall -Wextra -Wpedantic -std=f2003 -Wimplicit-interface src/*Component.f03 src/*CLI.f03 src/Main.f03 -o target/cli/simplecrop
elif [[ "$ENVIRONMENT" == "lib" ]]; then
  mkdir -p target/lib
  gfortran -Wall -Wextra -Wpedantic -std=f2003 -Wimplicit-interface src/*Component.f03 src/*CLI.f03 src/Main.f03 -c
  ar crs libsimplecrop.a *.o
  mv libsimplecrop.a target/lib
fi
