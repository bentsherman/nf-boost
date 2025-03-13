# nf-boost

<p align="center">
  <em>while we wait for <a href="https://github.com/nextflow-io/nextflow/issues/452">four five two</a></em>
  <br>
  <em>a very special plugin, just for you</em>
</p>

Nextflow plugin for experimental features that want to become core features!

Currently includes the following features:

- automatic deletion of temporary files (`boost.cleanup`)

- `fromJson` and `toJson` functions to read and write JSON

- `fromYaml` and `toYaml` functions to read and write YAML

- `mergeCsv` function for saving records to a CSV file

- `mergeText` function for saving items to a text file (similar to `collectFile` operator)

- `request` function for making HTTP requests

- `template` function for rendering templates

- `scan` operator for, well, scan operations

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

The plugin requires Nextflow version `23.10.0` or later.

*New in version `0.4.0`: requires Nextflow `24.04.0` or later.*

If a release hasn't been published to the main registry yet, you can still use it by specifying the following environment variable so that Nextflow can find the plugin:

```bash
export NXF_PLUGINS_TEST_REPOSITORY="https://github.com/bentsherman/nf-boost/releases/download/0.3.2/nf-boost-0.3.2-meta.json"
```

## Examples

Check out the `examples` directory for example pipelines that demonstrate how to use the features in this plugin.

## Reference

### Configuration

**`boost.cleanup`**

Set to `true` to enable automatic cleanup (default: `false`). Temporary files will be automatically deleted as soon as they are no longer needed.

The default cleanup observer uses `publishDir` directives to determine whether a file should be published before it is deleted. Setting `boost.cleanup = 'v2'` will use an alternate cleanup observer which uses the new workflow publish definition instead of `publishDir` to track publishing.

Limitations:

- Resume is not supported with automatic cleanup at this time. Deleted tasks will be re-executed on a resumed run. Resume will be supported when this feature is finalized in Nextflow.

- Helper files and log files created by Nextflow (e.g. `.command.run`, `.command.log`) are not deleted. Consider using a cleanup policy on the underlying filesystem or object storage to delete these files automatically over time.

- Input files that are staged into the work directory (e.g. from an HTTP/FTP server or S3 bucket) are not deleted.

- Files created by operators (e.g. `collectFile`, `splitFastq`) cannot be tracked and so are not deleted. For optimal performance, consider refactoring such operators into processes:

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

**`boost.cleanupInterval`**

Specify how often to scan for cleanup (default: `'60s'`).

### Functions

**`fromJson( source: Path | String )`**

Load a value from JSON.

**`toJson( value, pretty = false ) -> String`**

Convert a value to JSON.

**`fromYaml( source: Path | String )`**

Load a value from YAML.

**`toYaml( value ) -> String`**

Convert a value to YAML.

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

**`request( url: String, [opts] )`**

Make an HTTP request. Returns the underlying [HttpURLConnection](https://docs.oracle.com/javase/8/docs/api/java/net/HttpURLConnection.html).

Available options:

- `body: String`

  The request body.

- `headers: Map<String,String>`

  Map of request headers.

- `method: String`

  The request method, can be `'get'`, `'post'`, `'head'`, `'options'`, `'put'`, `'delete'`, or `'trace'` (default: `'get'`).

**`template( source: Path | String, binding: Map ) -> String`**

Render a template with the given binding.

### Operators

**`exec( name, body )`**

The `exec` operator was removed in version 0.5.0. Use an `exec` process instead.

**`scan( [seed], accumulator )`**

The `scan` operator is similar to `reduce` -- it applies an accumulator function sequentially to each value in a channel -- however, whereas `reduce` only emits the final result, `scan` emits each partially accumulated value.

**`then( onNext, [opts] )`**

**`then( [others...], opts )`**

The `then` operator is a generic operator that can be used to implement nearly any operator you can imagine.

It accepts any of three event handlers: `onNext`, `onComplete`, and `onError` (similar to `subscribe`). Each event handler has access to the following methods:

- `emit( value )`: emit a value to the output channel

- `done()`: signal that no more values will be emitted

When there is only one source channel, the `done()` method will be called automatically when the source channel sends the `onComplete` event. You can still call it manually, e.g. to finalize the output earlier. When there are multiple source channels, you are responsible for calling `done()` at the appropriate time -- if you don't call it, your operator will wait forever.

When there are multiple source channels, `onNext` and `onComplete` events are *synchronized*. This way, you don't need to worry about making your event handlers thread-safe, because they will be invoked on one event at a time.

Available options:

- `onNext( value, [i] )`: Closure that is invoked when a value is emitted by a source channel. Equivalent to providing a closure as the first argument. When there are multiple source channels, the closure is invoked with a second argument corresponding to the index of the source channel.

- `onComplete( [i] )`: Closure that is invoked after the last value is emitted by a source channel. When there are multiple source channels, the closure is invoked with the index of the source channel.

- `onError( error )`: Closure that is invoked when an exception is raised while handling an `onNext` event. It is invoked the exception that caused the error. No further calls will be made to `onNext` or `onComplete` after this event. By default, the error is logged and the workflow is terminated.

- `singleton`: Whether the output channel should be a value (i.e. *singleton*) channel. By default, it is determined by the source channel, i.e. if the source is a value channel then the output will also be a value channel and vice versa.

## Development

Build and install the plugin to your local environment:

```bash
make install
```

Run with Nextflow as usual:

```bash
nextflow run hello -plugins nf-boost@<version>
```
