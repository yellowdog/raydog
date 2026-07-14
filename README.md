# RayDog: Building Ray Clusters with YellowDog

Please see the [overview documentation](docs/index.rst) here or the formatted version at: https://docs.yellowdog.ai/raydog/index.html.

## Building node machine images

The `packer/` directory contains a Packer template for baking AWS AMIs with
the YellowDog agent, Python, Ray, and RayDog pre-installed, which
substantially reduces node boot time compared with installing everything via
userdata. See [packer/README.md](packer/README.md).
