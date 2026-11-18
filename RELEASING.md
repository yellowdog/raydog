# Releasing RayDog

Releases are made locally. A release is one command, run from `next-version`:

```commandline
make release
```

The script, `bin/release.py` (standard library only), does everything except telling the operations team: it sets the release version and dates the changelog, moves `main` up to `next-version`, builds, tags, uploads to PyPI, publishes the documentation image, and starts the next development version on `next-version`. GitHub Actions runs the checks on every push and pull request, but takes no part in a release and holds no publishing credentials.

## Branches

- Work is merged into `next-version`. Every push and pull request runs the checks in `.github/workflows/ci.yml`: lint, type checking, the offline tests across the Python and Ray matrix, a package build, and a documentation build.
- `main` always matches the latest release on PyPI. Only the release script moves it, by fast-forwarding it to the release commit.

## Before you start

You need:

- `twine` and Docker, and to be signed in to the YellowDog Docker Hub account.
- `~/.pypirc` defining the repositories `yellowdog-ray` (PyPI) and `yellowdog-testpypi` (Test PyPI), with their credentials. The script pipes twine's output, so a credential prompt would not be visible and the upload would appear to hang.
- The development environment active (`source .venv/bin/activate`, after `make install`), since the script runs `make check` and `make build`.
- `next-version` checked out, clean, and pushed. Untracked files are ignored.
- On `next-version`: `__version__` in `src/yellowdog_ray/__init__.py` is the version to release with a `.dev` suffix (for example `3.0.0.dev0`), and `CHANGELOG.md` has a heading for it marked `(unreleased)`:

  ```markdown
  ## 3.0.0 (unreleased)
  ```

- `main` has nothing that is not on `next-version`, so that `main` can be fast-forwarded.

Optionally, run the tests against a live YellowDog account first: `make test-online`, then `make test-instances` for the tests that start cloud instances (it ends with the sweep). See `tests/README.md`.

## Releasing

1. Optionally, rehearse without changing anything:

   ```commandline
   python bin/release.py --dry-run
   ```

   This runs every check and builds the release, prints the commands it would then run, and puts the version and changelog files back as they were.

2. Optionally, rehearse an upload to Test PyPI:

   ```commandline
   make release-test
   ```

   This uploads the release version to Test PyPI. Nothing is committed, pushed or tagged, and the files are put back afterwards. A version can only be uploaded to Test PyPI once.

3. Release:

   ```commandline
   make release
   ```

   The script shows the version, tag, target, documentation image and next development version, and asks you to type the version to confirm.

4. Tell the operations team that documentation `v<version>` is available, so that it can be deployed to docs.yellowdog.ai. The script reminds you when it finishes.

## What the script does

1. **Checks, before changing anything.** It stops if any of these fail:
   - you are on `next-version`;
   - after `git fetch`, `HEAD` equals `origin/next-version`;
   - no tracked files have uncommitted changes;
   - `origin/main`, and `main` if it exists locally, are ancestors of `HEAD`;
   - the release version is a final version such as `3.0.0`;
   - the tag `v<version>` exists neither locally nor on origin;
   - the version is not already on PyPI;
   - `make check` passes (lint, type checking, the offline tests and the documentation build).
2. **Confirmation.** You type the version (skip with `--yes`).
3. **Version and changelog.** Sets `__version__` to the release version and changes the heading `## <version> (unreleased)` to `## <version> (YYYY-MM-DD)`, using today's date.
4. **Build.** Runs `make clean build` and `twine check dist/*`. If either fails, the two files are put back and nothing has been committed.
5. **Commit and branches.** Commits the two files as `release: <version>`, pushes `next-version`, and pushes the same commit to `main` (`git push origin HEAD:main`, a fast-forward; git refuses anything else). A local `main`, if you have one, is moved to the same commit. You stay on `next-version` throughout.
6. **Publish.** Tags `v<version>`, uploads with `twine`, pushes the tag, and runs `make docs-publish-image`, which builds the documentation and pushes `yellowdogco/raydog-docs:<version>`.
7. **Next version.** Sets `__version__` to the next `.dev0` version (by default the next patch: `3.0.0` gives `3.0.1.dev0`), adds a `## <next> (unreleased)` changelog heading above the release's, commits it as `chore: start <next>.dev0`, and pushes `next-version`.

## Options

`python bin/release.py [options]`:

| Option | Effect |
|---|---|
| `--dry-run` | Checks and build only; prints the remaining commands. Changes nothing. |
| `--test-pypi` | Upload to Test PyPI. No commit, push, tag or documentation image. |
| `--version X.Y.Z` | Release this version instead of `__version__` without `.devN`. The changelog still needs a `## X.Y.Z (unreleased)` heading. |
| `--next-version X.Y.Z` | Start `X.Y.Z.dev0` afterwards instead of the next patch, for example `--next-version 3.1.0`. |
| `--skip-docs-image` | Do not publish the documentation image. |
| `--yes` | Do not ask for confirmation. |

`make release` and `make release-test` run the script with no options and with `--test-pypi`. To pass other options, run the script directly.

## When something fails

The script says what has and has not happened, and what to do next.

- **Before the release commit is pushed** (a failed check, build or commit), nothing has been published. Fix the cause and re-run.
- **The release commit was made but a push failed.** Push it with `git push origin next-version` and re-run. A re-run finds the version already set and the changelog already dated, so it reuses the commit.
- **The upload failed or was interrupted (including Ctrl-C).** The local tag is deleted. After an interrupt, check PyPI first: the upload may have completed. If it did not, re-run. If it did, follow the printed commands to push the tag.
- **After the upload** (pushing the tag or publishing the documentation image failed), the version is on PyPI and must not be uploaded again. Run the printed command (`git push origin v<version>` and/or `make docs-publish-image`), then start the next version by hand (below).
- **Starting the next version failed.** The release is complete. On `next-version`, set `__version__` to the next `.dev0` version, add a `## <next> (unreleased)` heading above the others in `CHANGELOG.md`, then commit and push.

Errors from failed commands include their output.

## Trying an unreleased version

`make release-test` puts the release version on Test PyPI. To put an unreleased version on a cluster's nodes, set `RAYDOG_PIP_SPEC` in the node setup scripts. It is passed to the installer as a single requirement, so it cannot carry an index option; give a direct reference instead, either to the wheel's URL on Test PyPI (`yellowdog-ray @ https://test-files.pythonhosted.org/.../yellowdog_ray-<version>-py3-none-any.whl`) or to a branch of a repository the nodes can read (`yellowdog-ray @ git+https://github.com/<owner>/<repo>@<branch>`).

The real-platform tests do not need this: they build a wheel and let Ray carry it to the head node.

## Versions that must move together

Ray on a user's machine must match Ray on the cluster's nodes. When the version of Ray used in the examples changes, change it in all of these: `RAY_VERSION` in `examples/autoscaler/scripts/node-setup.sh` and `examples/builder/scripts/node-setup-userdata.sh`; the image tag in the three `examples/autoscaler/scripts/*-docker.sh` files; the image tag in the two `examples/builder/scripts/*-docker.sh` files; `RAY_VERSION` in the `Makefile` (for `make update` and the Packer build) and the defaults in `packer/`; the `pip install` line in `docs/quickstart.rst`; and `RAY_LATEST` in `.github/workflows/ci.yml`.
