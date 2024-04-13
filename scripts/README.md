This directory contains scripts that can be used to benchmark the automatic cleanup. The `examples/cleanup.nf` script is an example pipeline, but you can use any pipeline you want.

Here's a simple benchmarking workflow:

1. Run `./watch baseline.txt` with automatic cleanup disabled. It will save the disk usage over time to `baseline.txt`.

2. Run `./watch cleanup.txt` with automatic cleanup enabled. It will save the disk usage over time to `cleanup.txt`.

3. Run `./plot baseline.txt cleanup.txt disk-usage.png` to produce a time series plot for the two runs. To run this script, you need Python 3 with the `matplotlib` and `pandas` packages.