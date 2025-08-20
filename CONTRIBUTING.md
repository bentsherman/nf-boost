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

1. In `build.gradle` make sure that:
   * `version` matches the desired release version,
   * `github.repository` matches the repository of the plugin,
   * `github.indexUrl` points to your fork of the plugins index repository.

2. Create a file named `$HOME/.gradle/gradle.properties`, where `$HOME` is your home directory. Add the following properties:

   * `github_username`: The GitHub username granting access to the plugin repository.
   * `github_access_token`: The GitHub access token required to upload and commit changes to the plugin repository.
   * `github_commit_email`: The email address associated with your GitHub account.

3. Update the [changelog](./CHANGELOG.md).

4. Build and publish the plugin to your GitHub repository:

   ```bash
   make release
   ```

5. Create a pull request against the [nextflow-io/plugins](https://github.com/nextflow-io/plugins/blob/main/plugins.json) repository to make the plugin publicly accessible.
