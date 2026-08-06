[![Release](https://img.shields.io/github/release/RedisGears/LibMR.svg?sort=semver)](https://github.com/RedisGears/LibMR/releases)
[![CircleCI](https://circleci.com/gh/RedisGears/LibMR/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGears/LibMR/tree/master)
[![codecov](https://codecov.io/gh/RedisGears/LibMR/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisGears/LibMRs)

# LibMR
[![Forum](https://img.shields.io/badge/Forum-RedisGears-blue)](https://forum.redislabs.com/c/modules/redisgears)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/6yaVTtp)

LibMR is a Map Reduce library that runs on top of Redis Cluster and agnostic to the deployment (oss/oss-cluster/enterpris-cluster).

# Build

## Prerequisite
The following list must be installed before compiling the library
- build-essential (or anything equavalent on you OS)
- autoconf
- libtool
- libssl-dev
- gcc-9 or above (or clang)

## Build
Clone the repository and perform the following to build the static library:

```
git submodule update --init --recursive
make build_deps
make MODULE_NAME=<module name as given to RedisModule_Init function>
```

If all is done correctly, `libmr.a` file should be located under `src` directory.

# Build Tests

## Prerequisite
Other then the build prerequisite, the following list must be installed to run the tests
- python3.6 or above
- RLTest - https://github.com/RedisLabsModules/RLTest
- gevents
- redis
- rust

## Build
Clone the repository and perform the following to build and run the tests:

```
git submodule update --init --recursive
make clean
make build_deps
make run_tests
```

Notice that running the test will compile MRLib with `MODULE_NAME=MRTESTS`, please do not use this compilation with any other module other then the tests module as it will not work.

## License

Starting with Redis 8, LibMR is licensed under your choice of: (i) Redis Source Available License 2.0 (RSALv2); (ii) the Server Side Public License v1 (SSPLv1); or (iii) the GNU Affero General Public License version 3 (AGPLv3). Please review the license folder for the full license terms and conditions. Prior versions remain subject to (i) and (ii).

## Code contributions


By contributing code to this Redis module in any form, including sending a pull request via GitHub, a code fragment or patch via private email or public discussion groups, you agree to release your code under the terms of the Redis Software Grant and Contributor License Agreement. Please see the CONTRIBUTING.md file in this source distribution for more information. For security bugs and vulnerabilities, please see SECURITY.md. 
