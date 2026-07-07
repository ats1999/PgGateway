## EC2 benchmarking (3 VMs)

You run the benchmark **from the client VM**. The client uses `pgbench` to drive load against:

- **direct postgres**: client → pg:5432
- **PgBouncer**: client → pooler:6433 → pg:5432
- **pg-bouncer-rs**: client → pooler:6432 → pg:5432

During the run we collect:

- **OS metrics** on pooler + pg (and optionally client): `vmstat`, `mpstat`, `iostat`, `sar -n DEV`
- **Process metrics**:
  - pooler: `pidstat` for the pooler PID
  - pg: an aggregate sampler for processes named `postgres`

### Security groups / ports

- **SSH**: client → pooler:22, client → pg:22
- **Benchmark traffic**:
  - client → pg:5432 (for direct run + `pgbench -i`)
  - client → pooler:6432 and 6433
  - pooler → pg:5432

Keep ports restricted to the relevant security group(s) (client SG, pooler SG).

### Instance setup (Ubuntu example)

On **client**:

- Packages: `postgresql-client` (for `psql`, `pgbench`), `openssh-client`

On **pooler**:

- Packages: `pgbouncer`, `sysstat` (for `pidstat/mpstat/iostat/sar`), `postgresql-client` (for readiness checks)
- Install `pg-bouncer-rs` binary (any one):
  - put a release binary on the box and set `PG_BOUNCER_RS_REMOTE_BIN=/path/to/pg-bouncer-rs`
  - or ensure `pg-bouncer-rs` is in `PATH`

On **pg**:

- Postgres running and listening on the private IP
- `pg_hba.conf` allowing auth from client + pooler private subnets/SG
- Packages: `sysstat`

### Run

From the repo root on the **client** VM:

```bash
chmod +x scripts/ec2/run_benchmark_ec2.sh scripts/ec2/remote/*.sh

scripts/ec2/run_benchmark_ec2.sh \
  --ssh-user ubuntu \
  --ssh-key ~/.ssh/bench.pem \
  --pooler-host 10.0.1.10 \
  --pg-host 10.0.2.10 \
  --pg-user postgres --pg-pass postgres \
  --db-name pgbench \
  --clients 64 --jobs 8 --duration 60 --scale 10
```

Outputs go to:

- `benchmark-results/ec2/<run_id>/pgbench_*.txt`
- `benchmark-results/ec2/<run_id>/pooler.tar.gz` (pooler logs + metrics)
- `benchmark-results/ec2/<run_id>/pg.tar.gz` (pg metrics)

To inspect:

```bash
cd benchmark-results/ec2/<run_id>
tar -xzf pooler.tar.gz
tar -xzf pg.tar.gz
```
