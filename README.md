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

## Requirements

| Version |	Minimum Nextflow version |
| ------- | ------------------------ |
| 0.8.x   | 25.10 |
| 0.7.x   | 25.10 |
| 0.6.x   | 25.04 |
| 0.5.x   | 24.10 |
| 0.4.x   | 24.04 |
| 0.1.x   | 23.10 |

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

- Files that are published as symlinks (the default publish mode) are not deleted, since deleting them would break the published files. Use the `copy` or `link` publish mode to allow these files to be cleaned up.

- Input files that are staged into the work directory (e.g. from an HTTP/FTP server or S3 bucket) are not deleted.

- Files created by operators (e.g. `collectFile`, `splitFastq`) cannot be tracked and so are not deleted. For optimal performance, consider refactoring such operators into processes:

  - Splitter operators such as `splitFastq` can also be used as functions in a native process:

    ```groovy
    process SPLIT_FASTQ {
      input:
      path(fastq)

      output:
      path(chunks)

      exec:
      chunks = fastq.splitFastq(file: true)
    }
    ```

  - The `collectFile` operator can be replaced with `mergeText` (in this plugin) in a native process. See the `examples` directory for example usage.

**`boost.cleanupFailedTasks`**

When `true`, delete the work directory of each failed task attempt that is retried (default: `false`). Requires `boost.cleanup` to be enabled. The error is still reported in the log, but the task files (e.g. `.command.err`) will not be available for debugging.

**`boost.cleanupInterval`**

Specify how often to scan for cleanup (default: `'60s'`).

### Functions

**`fromJson( source: Path | String )`**

Load a value from JSON.

**`toJson( value, pretty: boolean = false ) -> String`**

Convert a value to JSON.

**`fromYaml( source: Path | String )`**

Load a value from YAML.

**`toYaml( value ) -> String`**

Convert a value to YAML.

**`mergeCsv( records: List, path: Path, [opts] )`**

Save a list of records (i.e. tuples or maps) to a CSV file.

Available options:

- `header: boolean | List<String>`

  When `true`, the keys of the first record are used as the column names (default: `false`). Can also be a list of column names.

- `sep: String`

  The character used to separate values (default: `','`).

**`mergeText( items: List<Path> | List<String>, path: Path, [opts] )`**

Save a list of items (i.e. files or strings) to a text file.

Available options:

- `keepHeader: boolean`

  Prepend the resulting file with the header of the first file (default: `false`). The number of header lines can be specified using the `skip` option, to determine how many lines to remove from each file.

- `newLine: boolean`

  Append a newline character after each entry (default: `false`).

- `skip: int`

  The number of lines to skip at the beginning of each entry (default: `1` when `keepHeader` is true, `0` otherwise).

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

**`then( ... )`**

The `then` operator was removed in version 0.8.0.

## License

This project is licensed under the Apache License 2.0. See [COPYING](COPYING) for details.
