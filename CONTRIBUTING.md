# nf-boost

Contributions are welcome. Fork [this repository](https://github.com/nextflow-io/nf-boost) and open a pull request to propose changes. Consider submitting an [issue](https://github.com/nextflow-io/nf-boost/issues/new) to discuss any proposed changes with the maintainers before submitting a pull request.

## Development

Build and install the plugin to your local environment:

```bash
make install
```

Run with Nextflow as usual:

```bash
nextflow run hello -plugins nf-boost@<version>
```

## Publishing

Follow these steps to package, upload, and publish the plugin:

1. Update the version in `build.gradle`.

2. Make a release commit, e.g. "Release 0.1.0".

3. Run `make release` to build and publish the plugin.

4. Make a [GitHub release](https://github.com/nextflow-io/nf-boost/releases).
