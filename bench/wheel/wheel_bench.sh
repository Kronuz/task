#!/usr/bin/env bash
# The wheel-evidence rows for task::timer_wheel (wheel_bench.cc):
#   wheel_bench.sh single [R]   R interleaved rounds (default 10) of a a2 f t at n = 1e3 and 1e6
#   wheel_bench.sh cross [R]    R interleaved rounds (default 5) of f f2 t at P = 1, 4 and 14
#   wheel_bench.sh pads         the placement probe: a and f at n = 1e3, 3 rounds, with
#                               $PADS bytes between the wheel and the entries
# $CANDIDATES narrows the arms (a2 follows a, f2 follows f) and $PRODUCERS the cross row's P.
# Each row prints its runs and then the medians, min-max, A/A band and ratios
# (wheel_bench_summary.py).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
mode=${1:?mode}
build=${BUILD_DIR:-$root/.scratch/wheel-bench}
mkdir -p "$build"
echo "host=$(uname -sm) cpu=\"$(sysctl -n machdep.cpu.brand_string 2>/dev/null || sed -n 's/^model name\t*: //p' /proc/cpuinfo | head -1)\" hw_threads=$(getconf _NPROCESSORS_ONLN) compiler=\"$(c++ --version | head -1)\" commit=$(git -C "$root" rev-parse --short=12 HEAD 2>/dev/null || echo none)"
c++ -std=c++20 -pthread -O2 -DNDEBUG -I"$here" -I"$root" "$here/wheel_bench.cc" -o "$build/timing"
log="$build/$mode.log"
: >"$log"
run() { "$build/timing" "$@" | tee -a "$log"; }
with_pairs() {  # the arms, each A/A twin after its arm
	local out=() arm
	for arm in "$@"; do
		out+=("$arm")
		[ "$arm" != "$1" ] || out+=("${arm}2")
	done
	echo "${out[@]}"
}
echo "start=$(date -u +%FT%TZ)"
case $mode in
single)
	rounds=${2:-10}
	read -ra arms <<<"$(with_pairs ${CANDIDATES:-a f t})"
	for ((r = 0; r < rounds; ++r)); do
		for ((k = 0; k < ${#arms[@]}; ++k)); do
			arm=${arms[$(((k + r) % ${#arms[@]}))]}
			echo "round=$r"
			run single "$arm" 1000
			run single "$arm" 1000000
		done
	done
	;;
cross)
	rounds=${2:-5}
	read -ra arms <<<"$(with_pairs ${CANDIDATES:-f t})"
	read -ra producers <<<"${PRODUCERS:-1 4 14}"
	for ((r = 0; r < rounds; ++r)); do
		for ((k = 0; k < ${#arms[@]}; ++k)); do
			arm=${arms[$(((k + r) % ${#arms[@]}))]}
			echo "round=$r"
			for p in "${producers[@]}"; do run cross "$arm" "$p" 1000000; done
		done
	done
	;;
pads)
	read -ra pads <<<"${PADS:-0 4096 8192 12288 16384 20480 24576 28672 32768 36864 40960 45056 49152 53248 57344 61440}"
	for r in 0 1 2; do
		for pad in "${pads[@]}"; do
			for arm in ${CANDIDATES:-a f}; do
				echo "round=$r"
				WHEEL_BENCH_PAD=$pad run single "$arm" 1000
			done
		done
	done
	;;
*)
	echo "unknown mode $mode" >&2
	exit 2
	;;
esac
echo "end=$(date -u +%FT%TZ)"
python3 "$here/wheel_bench_summary.py" "$log"
