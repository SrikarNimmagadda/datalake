# Tech Brands Data Lake Application

This repository contains all of the files neccessary to deploy an s3-based
EMR data lake for Tech Brands (with the exception of the EMR cluster itself
(see [the EMR repository](https://github.com/GameStopCorp/tb.emr.datalake)).

**NOTE:** this section needs a link to the solution architecture documentation.

## Lambdas

The lambda function code is gathered under the `functions` sub-directory,
with a separate sub-directory for each lambda. Unit tests are stored
in a `tests` folder within each lambda folder.

The lambdas all pass linting, so the build will fail if a linting rule
is broken on lambda code.

### route_raw

The `route_raw` lambda is triggered by files dropping in to the landing bucket.
The lambda routes the files, based on their filename prefix, to one of the
raw buckets.

### extract_metadata

The `extract_metadata` lambda is triggered by files dropping into any of the
raw buckets or into the delivery bucket. This lambda then writes the
metadata of those files to the file metadata dynamodb table.

### add_jobflow_steps

The `add_jobflow_steps` family of lambdas is the function that sends work
to the EMR cluster, based on schedules. However, even though we only have
one folder for `add_jobflow_steps`, we can create many lambdas that can
run on different schedules and send different work into the cluster
using the same codebase for each lambda.

For more information, see the [README](functions/add_jobflow_steps/README.md)
in the function's folder.

## Spark Code

The majority of the files for this application are stored in the `spark`
directory. These files are the real meat and potatoes of the application.

Currently, there are thousands of linting errors coming from the spark
code, so, while the build pipeline still runs the linter for these files,
failed linting will not break the build. I would like to get to a clean
lint across all files eventually--it's amazing how many real errors a
linter can dig up for you.

## Automation

### The CI/CD Pipeline

Our pipeline consists of 3 main stages: [commit](#commit-stage), [functional acceptance test](#functional-acceptance-test-stage),
and [deploy](#deployment-stage).

#### Commit Stage

In the commit stage, we do our linting and unit testing, and we package
the application for deployment.

The linter will hurt your feelings but make you a better developer.
Follow the rules it lays down unless you have a very good reason for
breaking them.

Unit tests are our first line of defense against poor quality. Much of
the codebase is data access code, which is notoriously difficult or
impossible to unit test, but even those files may have pieces that are
unit testable, and those parts of the code should have tests.

It is a best practice to run the commit stage locally before pushing your
code. This practice minimizes broken builds and helps you find problems
faster. Keep reading for details on how to run the commit stage locally.
The [make](#make) and [docker](#working-locally-with-docker) sections will be especially important.

#### Functional Acceptance Test Stage

Functional acceptance tests require the application to be installed, so
our pipeline deploys the application with all of the resource names suffixed
with 'test' and then runs the defined integration tests.

See the [functional acceptance tests README](./tests/functional-acceptance-tests)

#### Deployment Stage

The final stage of the pipeline is deployment. If all previous stages have
passed, then the pipeline will deploy the resources and code.

### Serverless Framework

Not only is the code stored in this repository, but also the definition
of all of the resources needed to run it in any environment (with the
exception of the EMR cluster itself, which can be shared by all instances
of this application in the same account, so must be deployed separately).

The resource definitions are declared in `serverless.yml`, which is the
file used by the [serverless framework](https://serverless.com). We lean
heavily on this framework to help automate our deployment of aws resources.
Though it was originally intended as a helper to deploy lambdas, you can
put cloudformation inside it and serverless will deploy any aws resource
that you can define with cloudformation templates.

**TODO:** The current `serverless.yml` file is getting fairly unwieldy--I'd like
to break it down into sub-files.

### Make

Though it is fairly old-fashioned, it is also a very straighforward tool.
We are using `make` to act as an entry point into any automation steps, and
it also serves as a nice place to add conveniences that save you a lot of
typing long commands.

WARNING: Because the makefile is primarily a tool for automation, which runs
from a linux box, some commands will only work on a linux/mac machine. You
may have varying success on a windows machine depending on which tools you
have installed to emulate being on a linux machine. See the
(Working Locally with Docker) section for the best way to run make on windows.

Also, make is not installed on windows by default, but [chocolatey has a package](https://chocolatey.org/packages/make) for it, and if you already have [chocolatey](https://chocolatey.org/), it's a cinch to install.

The best way to use the makefile is to run in a docker container. See the
next section for more details.

A makefile is basically just a list of rules--The rules are the words
before the colons. Each rule may have dependencies on other rules (listed
after the colon.)

When you run a rule, it will run all of its dependent rules first, and
these dependencies can chain. The rules and dependencies can be filenames,
and make is smart about not running rules whose dependent files haven't
changed--but we're not using make like that--that's for c programmers.

To run the entire commit stage of the build pipeline, which runs
bootstrapping, linting, unit testing, and packaging, just type

```sh
make
```

The first rule in the makefile gets run if you don't specify a rule. Our
commit stage rule is first in the list, so that is our default.

You can run your unit tests with the following command:

```sh
make test
```

...run linting with

```sh
make lint
```

...or you can lint just the lambdas or just the spark code.

```sh
make lint-lambdas
make lint-spark
```

There are currently lots of rules defined. The ones suffixed with `-stage`
are called by the CodePipeline automation. See the [makefile](makefile)
for more details.

## Working Locally with Docker

I've found that docker is very helpful with emulating the environment we'll
be running in (especially when working on the build automation).

If you have Windows 10, you can install [Docker for Windows](https://www.docker.com/docker-windows) and everything is pretty straightforward.
Windows 7 or 8 is [a sadder story](https://docs.docker.com/toolbox/toolbox_install_windows/), but still possible.

Once you have docker installed, to fire up the environment, from a powershell
command line, type

```ps1
bin/run-dev-env
```

The first time you do this, it will take a while as it downloads the base
images. However these will be cached, and it will fire up quickly on
subsequent calls. The aforementioned script binds your local work environment
to the docker container's filesystem, so you can change files and the
container will see the changes without restarting.

Once you're at the docker prompt, you can start typing `make` commands.
The bootstrap rule will install pipenv and all your dependencies.

```sh
make bootstrap
```

I generally either run that one, or just run `make`, which runs the entire
commit pipeline stage step, when I first start the container.

## A Word on Authentication

When working on the serverless.yml file, it is helpful to be able to run
`sls package` before pushing the code to make sure that you don't have
any typos in the file that will cause serverless to fail to build its
output. However, even just to package, serverless needs to authenticate
to AWS.

See [the AWS CLI User Guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
for details on how to set up your command line with credentials.

If you are using docker, the `bin/run-dev-env.ps1` script passes all of the
AWS environment variables through to the container, so if you're using
docker, set the environment variables if you need to authenticate inside
of the container.

## Getting the Application Deployed

The application should only be deployed to non-ephemeral environments by the automation suite. Changes to the
master branch are deployed automatically to the d0063 account, with all
resources suffixed by the branch name. You can deploy other branches as
well for testing by changing the [`deploy.sh`](https://github.com/GameStopCorp/tb.aws.pipelines.datalake/blob/master/app/tb.app.datalake/deploy.sh) file in the [`app/tb.app.datalake`](https://github.com/GameStopCorp/tb.aws.pipelines.datalake/tree/master/app/tb.app.datalake) directory of the [pipelines repository](https://github.com/GameStopCorp/tb.aws.pipelines.datalake).
Be careful to set up the environment variables correctly in that file for
your deployment.

If you just need to test some changes in AWS, fire up the docker container while you have your development branch is current and run the pipeline rules from the makefile.

```sh
make commit-stage
make deploy
```

will build and deploy the application for your branch. Be sure to remove it when you are done with

```sh
make remove
```

If you have functional tests written for your changes, you can do

```sh
make commit-stage
make test-stage
```

and the test stage will deploy, run the functional tests and remove the application when it is done. These are the same steps that will be run by the automation, and you'll be running them in the exact same container that CodeBuild is using.

Also, use the same Pull Request process as usual to make changes to the pipelines repository.

For permanent deployments like QA and Prod, we will have specific long-lived
branches. You should never commit code changes to these branches, but only
use them to merge from master, which will cause a deployment through the
automation when we are ready.

Removing old deployments is currently somewhat painful.

**TODO:** Script removal and document it.
