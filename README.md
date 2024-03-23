# nf-boost

<p align="center">
  <em>while we wait for <a href="https://github.com/nextflow-io/nextflow/issues/452">four five two</a></em>
  <br>
  <em>here is a very special plugin, just for you</em>
</p>

Nextflow plugin for experimental features that want to become core features!

Currently includes the following features:

- automatic deletion of temporary files (`boost.cleanup`)

- `mergeCsv` function for saving records to a CSV file

- `mergeText` function for saving items to a text file (similar to `collectFile` operator)

- `then` operator for defining custom operators in your pipeline

## Getting Started

To use `nf-boost`, include it in your Nextflow config and add any desired settings:

```groovy
plugins {
  id 'nf-boost'
}

boost {
  cleanup = true
}
```

This plugin hasn't been published to the main registry yet, so you'll also need to specify the following environment variable so that Nextflow can find the plugin:

```bash
export NXF_PLUGINS_TEST_REPOSITORY="https://github.com/bentsherman/nf-boost/releases/download/0.1.0/nf-boost-0.1.0-meta.json"
```

The plugin requires Nextflow version `23.10.0` or later.

## Examples

Check out the `examples` directory for example pipelines that demonstrate how to use the features in this plugin.

## Reference

### Configuration

**`boost.cleanup`**

Set to `true` to enable automatic cleanup (default: `false`).

Temporary files will be automatically deleted as soon as they are no longer needed. Additionally, each task directory will be deleted as soon as the task outputs are no longer needed.

*NOTE: Resume is not supported with automatic cleanup. Deleted tasks will be re-executed on a resumed run.*

### Functions

**`mergeCsv( records, path, [opts] )`**

Save a list of records (i.e. tuples or maps) to a CSV file.

Available options:

- `header`: When `true`, the keys of the first record are used as the column names (default: `false`). Can also be a list of column names.

- `sep`: The character used to separate values (default: `','`).

**`mergeText( items, path, [opts] )`**

Save a list of items (i.e. files or strings) to a text file.

Available options:

- `keepHeader`: Prepend the resulting file with the header of the first file (default: `false`). The number of header lines can be specified using the `skip` option, to determine how many lines to remove from each file.

- `newLine`: Append a newline character after each entry (default: `false`).

- `skip`: The number of lines to skip at the beginning of each entry (default: `1` when `keepHeader` is true, `0` otherwise).

### Operators

**`then( closure, [opts] )`**

**`then( events, [opts] )`**

The `then` operator is a generic operator that can be used to implement any operator you can imagine.

It accepts any of three event handlers: `onNext`, `onComplete`, and `onError` (similar to `subscribe`). However, each event handler has an `emit()` method with which it can emit items to an output channel.

Available options:

- `singleton`: Whether the output channel should be a value (i.e. *singleton*) channel. By default, it is determined by the source channel, i.e. if the source is a value channel then the output will also be a value channel and vice versa.

## Development

The easiest way to build and test nf-boost locally is to run `make install`. This will build the plugin and install it to your Nextflow plugins directory (e.g. `~/.nextflow/plugins`), using the version defined in `MANIFEST.MF`. Finally, specify the plugin in your Nextflow config with this exact version. You can then use it locally like a regular plugin.

Refer to the [nf-hello](https://github.com/nextflow-io/nf-hello) README for more information about building and publishing Nextflow plugins.
