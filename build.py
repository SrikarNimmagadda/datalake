from pybuilder.core import task, use_plugin, init
from pybuilder.vcs import VCSRevision

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.distutils")
use_plugin("python.flake8")
#use_plugin('python.coverage')
use_plugin("pybuilder_aws_plugin")
use_plugin("exec")

name = "tb.app.datalake"
extract_metadata_path = "tb-app-datalake-extract-metadata"
route_raw_path = "tb-app-datalake-route-raw"
start_job_store_path = "tb-app-datalake-start-job-store"
version = VCSRevision().get_git_revision_count()
default_task = ["publish","package_lambda_code"]

@init
def set_properties(project):
    project.set_property("dir_source_main_python", "functions/")
    project.set_property("dir_source_unittest_python", "tests/")
    project.set_property("dir_source_main_scripts", "scripts/")

@task
def extract_metadata(project, logger):
# extract-metadata
    logger.info("I am building extract-metadata for {0} in version {1}!".format(project.name, project.version))
    project.set_property("dir_source_main_python", "functions/extract-metadata/")
    project.set_property("dir_source_unittest_python", "functions/extract-metadata/tests/")
    project.set_property("dir_source_main_scripts", "scripts/")
    project.set_property("dir_dist", "_build/dist/{0}-{1}".format(extract_metadata_path,version))

@task
def route_raw(project, logger):
# route-raw
    logger.info("I am building route-raw for {0} in version {1}!".format(project.name, project.version))
    project.set_property("dir_source_main_python", "functions/route-raw/")
    project.set_property("dir_source_unittest_python", "functions/route-raw/tests/")
    project.set_property("dir_source_main_scripts", "scripts/")
    project.set_property("dir_dist", "_build/dist/{0}-{1}".format(route_raw_path,version))

@task
def start_job_store(project, logger):
# start-job-store
    logger.info("I am building start_job_store for {0} in version {1}!".format(project.name, project.version))
    project.set_property("dir_source_main_python", "functions/start-job-store/")
    project.set_property("dir_source_unittest_python", "functions/start-job-store/tests/")
    project.set_property("dir_source_main_scripts", "scripts/")
    project.set_property("dir_dist", "_build/dist/{0}-{1}".format(start_job_store_path,version))