#!/usr/bin/env bash
# Runs the shaded jar with the JVM flags Agrona requires on modern JDKs (and a few extra
# module opens kept for good measure).
#
#   tools/run.sh engine  --port 9001
#   tools/run.sh loadgen --port 9001 --conns 4 --rate 200000 --duration 10000
#   tools/run.sh bench   --ops 10000000 --scenario mixed
#
# Extra JVM flags (e.g. GC tuning) can be passed via the JVM_FLAGS environment variable:
#
#   JVM_FLAGS="-XX:+UseZGC -Xmx4g" tools/run.sh engine
set -eo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
set +u
source "$DIR/tools/env.sh"
set -u
JAR="${JAR:-$DIR/target/lowlat-exchange.jar}"

exec java \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens  java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens  java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens  java.base/java.lang=ALL-UNNAMED \
  --add-opens  java.base/java.util=ALL-UNNAMED \
  --add-opens  java.base/java.nio=ALL-UNNAMED \
  --add-opens  java.base/java.io=ALL-UNNAMED \
  ${JVM_FLAGS:-} \
  -jar "$JAR" "$@"
