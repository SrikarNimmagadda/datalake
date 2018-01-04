#!/bin/bash -x

# phases:

#  pre_build:
#    commands:
chmod +x *.sh

./prebuild.sh

#  build:
#    commands:
echo Build started on `date`
echo "installing dependencies"
pyb install_dependencies 
echo "executing initial build per lambda fucntion"
pyb "pkg_extract_metadata"
pyb "pkg_route_raw" 
pyb "pkg_start_job_store"


# hmm if serverless is doing the pull of dependencies on package, then the unit tests have to run against the package?
# or do we have to pull dependencies twice? That would break the principle of "build once"
#serverless package

#  post_build:
#    commands:
./test.sh