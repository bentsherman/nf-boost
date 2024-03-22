# nf-cleanup

*while we wait for [four five two](https://github.com/nextflow-io/nextflow/issues/452)*

*here is a very special plugin, just for you*

## Getting Started

To use `nf-cleanup`, simply include it in your Nextflow config:

```groovy
plugins {
  id 'nf-cleanup'
}
```

Finally, run your Nextflow pipeline. You do not need to modify your pipeline script in order to use the `nf-cleanup` plugin. The plugin will automatically delete temporary files as soon as they are no longer needed.

The plugin requires Nextflow version `23.10.0` or later.

## Development

Refer to the [nf-hello](https://github.com/nextflow-io/nf-hello) README for more information about building and publishing Nextflow plugins.
