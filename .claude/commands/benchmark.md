---
description: Benchmark nf-boost automatic cleanup on a real pipeline against a no-cleanup baseline.
disable-model-invocation: true
argument-hint: "<pipeline> [nf-boost version] [nextflow version]"
---

Benchmark the disk usage of nf-boost automatic cleanup on a real pipeline, with a baseline run and a cleanup run, and report the metrics.

Run the whole procedure below in a background subagent, passing it these instructions and the arguments. Relay its report when it finishes.

Arguments: $ARGUMENTS

The first argument is the pipeline to benchmark, e.g. `nf-core/rnaseq`. It is required: if it's missing, ask the user for it before starting the subagent. Run the pipeline with `-profile test,docker`, and pin the latest release whose manifest `nextflowVersion` accepts the chosen Nextflow version, using `-r <version>`.

## Defaults

Use these settings unless the arguments override them:

- **nf-boost:** the released version in `VERSION`, loaded with `-plugins nf-boost@<version>`. Benchmark a released plugin, not a local build; skip `scripts/benchmark` and `gradlew install`, since they install the working tree.
- **Nextflow:** the minimum version for that nf-boost release in the README requirements table, pinned with `NXF_VER`.

## Isolation

The user may have global Nextflow config for caching and resuming. To keep the runs independent of it:

- Put everything under `evals/<pipeline name>-<date>`. Give each run its own launch dir, `-w` work dir, and `--outdir`.
- Set `NXF_HOME` and `NXF_CACHE_DIR` to dirs under the bench dir for every run. Never pass `-resume`.
- Run one Nextflow run at a time, and start nothing else in the meantime.
- Leave the git repo untouched: the branch, the working tree, and the commits.

## Steps

1. Read `scripts/README.md`, `scripts/watch`, and `scripts/metrics`. Check that Docker works and resolve the exact pipeline, plugin, and Nextflow versions.
2. Choose the cleanup mode from the pinned pipeline release's code. Use `v1` (`boost.cleanup = 'v1'`) if its processes publish with `publishDir`, and `v2` (`boost.cleanup = 'v2'`) if it publishes with the workflow `output` block. Note the evidence for the report. Leave the other `boost` options at their defaults.
3. Run the **baseline** (no cleanup) under `scripts/watch`, using its `CMD` variable or a wrapper.
4. Run the **cleanup** run the same way, with the chosen cleanup mode.
5. Run `scripts/metrics` on both runs. Add `--plot` with a venv under the bench dir that has matplotlib and pandas.
6. Compare the published outputs of the two runs, as `scripts/benchmark` does. The file lists must match. The cleanup run must have no broken symlinks, and no empty files where the baseline's are non-empty.
7. Search the cleanup run's `.nextflow.log` for cleanup warnings, e.g. `published as symlinks` or `Unable to delete`.

If a run fails, report the error from its `.nextflow.log`. Retry only when the cause is clearly transient. The benchmark is done when both runs succeed and every item in the report below is filled in.

## Report

- the exact Nextflow, pipeline, and nf-boost versions;
- the cleanup mode and the evidence for it;
- the exact commands;
- a table of peak, final, and AUC (GB-hours) disk usage for baseline vs cleanup, with % reduction;
- the wall time of each run;
- the result of the published-output comparison;
- the cleanup warnings;
- the paths to the raw data and the plot;
- every adaptation or workaround in the scripts.
