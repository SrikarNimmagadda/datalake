# create an apphash.txt file containing the git commit hash of the current commit
# this is normally done in the deploy step of the scripts, but if we're running locally, we won't have a pipeline to query for this information
git rev-parse --short HEAD | Out-File apphash.txt -Encoding UTF8
# because Out-File writes files with CRLF endings, and we want to read this file in linux, we need to replace the line endings
$lf_content = [IO.File]::ReadAllText("apphash.txt") -replace "`r`n", "`n"
[IO.File]::WriteAllText("apphash.txt", $lf_content)

# Builds and runs a docker container
docker build -f docker/dev/Dockerfile -t tb-app-datalake-dev .
docker run `
    --rm `
    --mount type=bind,source="$(pwd)",target=/app `
    -e AWS_ACCESS_KEY_ID `
    -e AWS_SECRET_ACCESS_KEY `
    -e AWS_SESSION_TOKEN `
    -e AWS_DEFAULT_REGION `
    -e AWS_DEFAULT_OUTPUT `
    -it tb-app-datalake-dev bash

# since we only have a latest tag for our image, we generate some cruft when we recreate it. This line removes the cruft
echo "Pruning old images for this application"
docker system prune --force --filter label=application=tb-app-datalake-dev
