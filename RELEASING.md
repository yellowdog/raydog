# Releasing a new version of RayDog

When RayDog is ready for release:

1. Ensure the version number in [src/yellowdog_ray/__init__.py](src/yellowdog_ray/__init__.py) has been updated on the `next-version` branch
2. Merge all changes to the `main` branch from `next-version`
3. Tag the repository
4. Push the changes and the tags to `origin/main`
5. Upload the new package version to PyPI
6. Build the documentation container image and upload to Dockerhub

## 1. Tag the repository with the new version number

The repository can be tagged using:

```commandline
RAYDOG_VERSION=$(grep "__version__ =" src/yellowdog_ray/__init__.py | sed -E 's/.*"([^"]+)".*/\1/')
git tag -a v$RAYDOG_VERSION -m "Version $RAYDOG_VERSION"
```

Push the current state of the repository and its tags to GitHub.

## 2. Upload the updated package to PyPi

The [Makefile](Makefile) provides targets for uploading new package versions to PyPI. To upload to production PyPI:

```commandline
make pypi-prod-upload
```

It's possible first to check the build using `twine`:

```commandline
make pypi-check-build
```

It's also possible to test the upload against the Test PyPI server:

```commandline
make pypi-test-upload
```

## 2. Build the documentation and the documentation container image

To build the container image, Docker must be running, and you must be signed in to the YellowDog Dockerhub account.

The following will build the image and push to the YellowDog Dockerhub repository:

```html
make docs-publish-image
```

Images are tagged with the version of the provider.

Finally, **notify the operations team** that a new version of the documentation is available.
