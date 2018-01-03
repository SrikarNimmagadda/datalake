from subprocess import call
import os
from pybuilder.core import task, use_plugin, init, depends, description
from pybuilder.errors import BuildFailedException
import zipfile
import shutil
import importlib

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.distutils")
use_plugin("python.flake8")
use_plugin("python.pylint")
#use_plugin('python.coverage')
#use_plugin("pybuilder_aws_plugin")
use_plugin("exec")
use_plugin("source_distribution")
#use_plugin("python.integrationtest")

name = "tb.app.datalake"
extract_metadata_path = "tb-app-datalake-extract-metadata"
route_raw_path = "tb-app-datalake-route-raw"
start_job_store_path = "tb-app-datalake-start-job-store"
default_task = ["analyze", "publish"]
#, "package_lambda_code","pkg_extract_metadata", "pkg_route_raw", "pkg_start_job_store"

dependencies = [
    ('boto3', '==1.4.7')
]

deploy_stage = os.getenv('STAGE')
# Maybe this works better if I can pass in the function name from serverless??
#function_name = os.getenv('FUNCTION_NAME')

@init
def initialize(project):
    project.build_depends_on('boto3', '==1.4.7')
    project.set_property("dir_source_main_python", "functions/")
    project.set_property("dir_dist", "$dir_target/dist/")
    project.set_property("flake8_break_build", False)
    project.set_property('flake8_include_test_sources', True)
    project.set_property("flake8_ignore", "E501")
    project.set_property("coverage_threshold_warn", 10)
    project.set_property("coverage_break_build", False)
    project.set_property('integrationtest_inherit_environment', False)
    project.get_property("distutils_commands").append("bdist_egg")
    project.set_property("distutils_classifiers", [
       "Environment :: {0}".format(deploy_stage),
       "Intended Audience :: Developers",
       'Programming Language :: Python :: 2.7',
       "Topic :: Software Development :: Data Lake",
       "Topic :: Software Development :: EMR"])


@task
@description('Package extract-metadata for deployment')
def pkg_extract_metadata(project, logger):
    project.set_property("dir_dist", "$dir_target/dist/extract-metadata/")
    logger.info("I am building extract-metadata for {0}!".format(project.name))
    for names, _ in dependencies:
        vendor_path = os.path.join('target/dist/extract-metadata', names)
        if os.path.isdir(vendor_path):
            continue
        dep_path = os.path.abspath(os.path.dirname(importlib.import_module(names).__file__))
        shutil.copytree(dep_path, vendor_path)

    with zipfile.ZipFile('target/dist/tb-app-datalake-extract-metadata.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('target/dist/extract-metadata'):
            for file in files:
                zipf.write(os.path.join(root, file),
                           os.path.relpath(os.path.join(root, file), 'target/dist/extract-metadata'))

@task
@description('Package route-raw for deployment')
def pkg_route_raw(project, logger):
    project.set_property("dir_dist", "target/dist/route-raw/")
    logger.info("I am building route-raw for {0}!".format(project.name))
    for name, _ in dependencies:
        vendor_path = os.path.join('target/dist/route-raw', name)
        if os.path.isdir(vendor_path):
            continue
        dep_path = os.path.abspath(os.path.dirname(importlib.import_module(name).__file__))
        shutil.copytree(dep_path, vendor_path)

    with zipfile.ZipFile('target/dist/tb-app-datalake-route-raw.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('target/dist/route-raw'):
            for file in files:
                zipf.write(os.path.join(root, file),
                           os.path.relpath(os.path.join(root, file), 'target/dist/route-raw'))

@task
@description('Package start-job-store for deployment')
def pkg_start_job_store(project, logger):
    project.set_property("dir_dist", "target/dist/start-job-store/")
    logger.info("I am building start-job-store for {0}!".format(project.name))
    for name, _ in dependencies:
        vendor_path = os.path.join('target/dist/start-job-store', name)
        if os.path.isdir(vendor_path):
            continue
        dep_path = os.path.abspath(os.path.dirname(importlib.import_module(name).__file__))
        shutil.copytree(dep_path, vendor_path)

    with zipfile.ZipFile('target/dist/tb-app-datalake-start-job-store.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('target/dist/start-job-store'):
            for file in files:
                zipf.write(os.path.join(root, file),
                           os.path.relpath(os.path.join(root, file), 'target/dist/start-job-store'))


# Not used unless Serverless fails to do its part again
@task
@depends('package')
@description('Deploy the project to AWS')
def deploy(logger):
    if deploy_stage is not None:
        call_str = 'serverless deploy -s {0}'.format(deploy_stage)
    else:
        call_str = 'serverless deploy'
    ret = call(call_str, shell=True)
    if ret != 0:
        logger.error("Error deploying project to AWS")
        raise BuildFailedException