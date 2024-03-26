# nf-boost

<p align="center">
  <em>while we wait for <a href="https://github.com/nextflow-io/nextflow/issues/452">four five two</a></em>
  <br>
  <em>a very special plugin, just for you</em>
</p>

Nextflow plugin for experimental features that want to become core features!

Currently includes the following features:

- automatic deletion of temporary files (`boost.cleanup`)

- `exec` operator for creating an inline native (i.e. `exec`) process

- `mergeCsv` function for saving records to a CSV file

- `mergeText` function for saving items to a text file (similar to `collectFile` operator)

- `scan` operator for, well, scan operations

- `then` and `thenMany` operators for defining custom operators in your pipeline

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

The plugin requires Nextflow version `23.10.0` or later.

## Examples

Check out the `examples` directory for example pipelines that demonstrate how to use the features in this plugin.

## Reference

### Configuration

**`boost.cleanup`**

Set to `true` to enable automatic cleanup (default: `false`).

Temporary files will be automatically deleted as soon as they are no longer needed. Additionally, each task directory will be deleted as soon as the task outputs are no longer needed.

Limitations:

- Resume is not supported with automatic cleanup at this time. Deleted tasks will be re-executed on a resumed run. Resume will be supported when this feature is finalized in Nextflow.

- Files created by operators (e.g. `collectFile`, `splitFastq`) cannot be tracked and so will not be deleted. For optimal performance, consider refactoring such operators into processes:

  - Splitter operators such as `splitFastq` can also be used as functions in a native process:

    ```groovy
    process SPLIT_FASTQ {
      input:
      val(fastq)

      output:
      path(chunks)

      exec:
      chunks = splitFastq(fastq, file: true)
    }
    ```

  - The `collectFile` operator can be replaced with `mergeText` (in this plugin) in a native process. See the `examples` directory for example usage.

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

**`exec( name, body )`**

*Coming in 0.3.0*

The `exec` operator creates and invokes an inline native (i.e. `exec`) process with the given name, as well as a closure which corresponds to the `exec:` section of a native process.

The inline process can be configured from the config file like any other process, including the use of process selectors (i.e. `withName`).

Limitations:

- Inline process directives are not supported yet.

- The inline exec body should accept a single value and return a single value. Multiple inputs/outputs are not supported yet.

**`scan( [seed], accumulator )`**

*Coming in 0.3.0*

The `scan` operator is similar to `reduce` -- it applies an accumulator function sequentially to each value in a channel -- however, whereas `reduce` only emits the final result, `scan` emits each partially accumulated value.

**`then( onNext, [opts] )`**

**`then( opts )`**

**`thenMany( onNext, emits: <emits>, [opts] )`**

**`thenMany( emits: <emits>, opts )`**

The `then` operator is a generic operator that can be used to implement nearly any operator you can imagine.

It accepts any of three event handlers: `onNext`, `onComplete`, and `onError` (similar to `subscribe`). Each event handler has access to the following methods:

- `emit( value )`: emit a value to the output channel (used only by `then`)

- `emit( name, value )`: emit a value to an output channel (used only by `thenMany`)

- `done()`: signal that no more values will be emitted

Available options:

- `emits`: List of output channel names when using `thenMany`. Whereas `then` emits a single channel, `thenMany` emits a multi-channel output (similar to processes and workflows) where each output can be accessed by name.

- `onNext`: Closure that is invoked when a value is emitted. Equivalent to providing a closure as the first argument.

- `onComplete`: Closure that is invoked after the last value is emitted by the channel.

- `onError`: Closure that is invoked when an exception is raised while handling an `onNext` event. It will not make further calls to `onNext` or `onComplete`. The `onError` method takes as its parameter the `Throwable` that caused the error. By default, the error is logged and the workflow is terminated.

- `singleton`: Whether the output channel should be a value (i.e. *singleton*) channel. By default, it is determined by the source channel, i.e. if the source is a value channel then the output will also be a value channel and vice versa.

Limitations:

- Multiple inputs are not supported. Operators with multiple inputs tend to be more complex and not amenable to abstraction. If you need to implement such an operator, you can implement it the "conventional" way in a plugin.

## Development

The easiest way to build and test nf-boost locally is to run `make install`. This will build the plugin and install it to your Nextflow plugins directory (e.g. `~/.nextflow/plugins`), using the version defined in `MANIFEST.MF`. Finally, specify the plugin in your Nextflow config with this exact version. You can then use it locally like a regular plugin.

Refer to the [nf-hello](https://github.com/nextflow-io/nf-hello) README for more information about building and publishing Nextflow plugins.
