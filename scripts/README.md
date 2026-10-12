This directory contains scripts that can be used to benchmark the automatic cleanup.

Run the full benchmark from anywhere in the repo:

```bash
scripts/benchmark [outdir]
```

It installs the current build of the plugin, runs the cleanup example three times (once without cleanup, once with `cleanup-v1`, and once with `cleanup-v2`), and saves the disk usage over time and the metrics (`metrics.json`) to `outdir` (default: `benchmark`). It fails if either cleanup run doesn't reduce the final disk usage, peak disk usage, and area under the curve enough relative to the baseline.

The individual scripts can also be used on their own, from the repo root:

1. `scripts/watch <outfile>` runs a pipeline and saves the disk usage of the work directory over time. Set `CMD` to run a different pipeline (default: `examples/cleanup-v2.nf` with cleanup enabled).

2. `scripts/metrics <inputs...> [--plot <outfile>]` prints the peak disk usage, final disk usage, and area under the curve (GB-hours) of one or more runs as JSON. Use `--plot` to also save a time series plot, which requires the `matplotlib` and `pandas` packages:

   ```bash
   python3 -m venv .venv && .venv/bin/pip install matplotlib pandas
   .venv/bin/python scripts/metrics benchmark/baseline.txt benchmark/v1.txt benchmark/v2.txt --plot benchmark/disk-usage.png
   ```
