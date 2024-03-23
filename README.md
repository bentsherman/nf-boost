# nf-boost

<p align="center">
  <em>while we wait for <a href="https://github.com/nextflow-io/nextflow/issues/452">four five two</a></em>
  <br>
  <em>here is a very special plugin, just for you</em>
</p>

Nextflow plugin for experimental features that want to become core features!

Currently includes the following features:

- automatic deletion of temporary files (set `boost.cleanup = true` in your config)

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

## Reference

### Configuration

**`boost.cleanup`**

Set to `true` to enable automatic cleanup (default: `false`). Temporary files will be automatically deleted as soon as they are no longer needed.

*NOTE: Resume is not supported with automatic cleanup. Deleted tasks will be re-executed on a resumed run.*

## Development

The easiest way to build and test nf-boost locally is to run `make install`. This will build the plugin and install it to your Nextflow plugins directory (e.g. `~/.nextflow/plugins`), using the version defined in `MANIFEST.MF`. Finally, specify the plugin in your Nextflow config with this exact version. You can then use it locally like a regular plugin.

Refer to the [nf-hello](https://github.com/nextflow-io/nf-hello) README for more information about building and publishing Nextflow plugins.
