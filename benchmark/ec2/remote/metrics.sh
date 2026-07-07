#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  metrics.sh start --run-dir DIR [--interval SEC] [--pid PID | --process-name NAME]
  metrics.sh stop  --run-dir DIR

Writes:
  DIR/meta.txt
  DIR/os/{vmstat.log,mpstat.log,iostat.log,sar_net.log}
  DIR/process/{pidstat.log,process_agg.csv}
  DIR/pids.env
EOF
}

command_exists() { command -v "$1" >/dev/null 2>&1; }

write_meta() {
  local run_dir="$1"
  {
    echo "timestamp_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "hostname=$(hostname)"
    echo "kernel=$(uname -a)"
    if command_exists lsb_release; then
      echo "os=$(lsb_release -ds)"
    elif [[ -f /etc/os-release ]]; then
      # shellcheck disable=SC1091
      . /etc/os-release
      echo "os=${PRETTY_NAME:-unknown}"
    fi
    if command_exists uptime; then
      echo "uptime=$(uptime || true)"
    fi
  } >"$run_dir/meta.txt"
}

start_background() {
  local cmd="$1"
  local out_file="$2"
  bash -c "$cmd" >"$out_file" 2>&1 &
  echo $!
}

start_process_aggregator() {
  local process_name="$1"
  local interval_sec="$2"
  local out_csv="$3"

  {
    echo "timestamp_utc,process_name,process_count,total_cpu_percent,total_rss_kb"
    while true; do
      local ts
      ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
      ps -axo comm=,pcpu=,rss= | awk -v ts="$ts" -v name="$process_name" '
        $1==name { count+=1; cpu+=$2; rss+=$3 }
        END {
          if (count==0) { cpu=0; rss=0 }
          printf "%s,%s,%d,%.2f,%d\n", ts, name, count, cpu, rss
        }
      '
      sleep "$interval_sec"
    done
  } >>"$out_csv" &
  echo $!
}

subcommand="${1:-}"
shift || true

run_dir=""
interval_sec="1"
pid=""
process_name=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir)
      run_dir="${2:-}"
      shift 2
      ;;
    --interval)
      interval_sec="${2:-}"
      shift 2
      ;;
    --pid)
      pid="${2:-}"
      shift 2
      ;;
    --process-name)
      process_name="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$subcommand" == "start" ]]; then
  [[ -n "$run_dir" ]] || { echo "Missing --run-dir" >&2; usage >&2; exit 2; }
  mkdir -p "$run_dir/os" "$run_dir/process"
  write_meta "$run_dir"

  : >"$run_dir/pids.env"

  if command_exists vmstat; then
    vmstat_pid="$(start_background "vmstat ${interval_sec}" "$run_dir/os/vmstat.log")"
    echo "VMSTAT_PID=${vmstat_pid}" >>"$run_dir/pids.env"
  fi

  if command_exists mpstat; then
    mpstat_pid="$(start_background "mpstat -P ALL ${interval_sec}" "$run_dir/os/mpstat.log")"
    echo "MPSTAT_PID=${mpstat_pid}" >>"$run_dir/pids.env"
  fi

  if command_exists iostat; then
    iostat_pid="$(start_background "iostat -xz ${interval_sec}" "$run_dir/os/iostat.log")"
    echo "IOSTAT_PID=${iostat_pid}" >>"$run_dir/pids.env"
  fi

  if command_exists sar; then
    sar_pid="$(start_background "sar -n DEV ${interval_sec}" "$run_dir/os/sar_net.log")"
    echo "SAR_NET_PID=${sar_pid}" >>"$run_dir/pids.env"
  fi

  if [[ -n "$pid" ]]; then
    if command_exists pidstat; then
      pidstat_pid="$(start_background "pidstat -h -u -r -d -p ${pid} ${interval_sec}" "$run_dir/process/pidstat.log")"
      echo "PIDSTAT_PID=${pidstat_pid}" >>"$run_dir/pids.env"
    fi
  elif [[ -n "$process_name" ]]; then
    agg_pid="$(start_process_aggregator "$process_name" "$interval_sec" "$run_dir/process/process_agg.csv")"
    echo "PROCESS_AGG_PID=${agg_pid}" >>"$run_dir/pids.env"
  fi

  echo "ok run_dir=${run_dir}"
  exit 0
fi

if [[ "$subcommand" == "stop" ]]; then
  [[ -n "$run_dir" ]] || { echo "Missing --run-dir" >&2; usage >&2; exit 2; }
  [[ -f "$run_dir/pids.env" ]] || { echo "No pids.env at $run_dir" >&2; exit 2; }

  # shellcheck disable=SC1090
  . "$run_dir/pids.env"

  for var_name in VMSTAT_PID MPSTAT_PID IOSTAT_PID SAR_NET_PID PIDSTAT_PID PROCESS_AGG_PID; do
    pid_value="${!var_name:-}"
    if [[ -n "$pid_value" ]] && kill -0 "$pid_value" 2>/dev/null; then
      kill "$pid_value" 2>/dev/null || true
      wait "$pid_value" 2>/dev/null || true
    fi
  done

  echo "ok run_dir=${run_dir}"
  exit 0
fi

echo "Missing subcommand: start|stop" >&2
usage >&2
exit 2
