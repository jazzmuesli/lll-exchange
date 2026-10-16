#!/usr/bin/env bash
# Sourceable environment for the lowlat project.
# Usage:  source tools/env.sh
#
# Everything is installed *locally* under $HOME (no root required):
#   - zip        -> $HOME/.local/opt/zip   (symlinked into $HOME/.local/bin)
#   - SDKMAN     -> $HOME/.sdkman
#   - Java 27    -> $HOME/.sdkman/candidates/java/current
#   - Maven      -> $HOME/.sdkman/candidates/maven/current
export PATH="$HOME/.local/bin:$PATH"
export SDKMAN_DIR="$HOME/.sdkman"
if [[ -s "$SDKMAN_DIR/bin/sdkman-init.sh" ]]; then
  # shellcheck disable=SC1091
  source "$SDKMAN_DIR/bin/sdkman-init.sh"
fi
