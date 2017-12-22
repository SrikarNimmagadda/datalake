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
use_plugin("pybuilder_aws_plugin")
#use_plugin("exec")
#use_plugin("source_distribution")
#use_plugin("python.integrationtest")

name = "tb.app.datalake"
extract_metadata_path = "tb-app-datalake-extract-metadata"
route_raw_path = "tb-app-datalake-route-raw"
start_job_store_path = "tb-app-datalake-start-job-store"
default_task = ["analyze","publish","package_lambda_code"]

dependencies = [
    ('requests', '==2.11.1')
]

deploy_stage = os.getenv('STAGE')

@init
def initialize(project):
    project.build_depends_on('boto3', '==1.4.7')
    for named, version in dependencies:
        project.depends_on(named, version)
    project.set_property("dir_dist", "$dir_target/dist/{0}".format(extract_metadata_path))
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
@description("Package the function Extract Metadata for deployment")
def pkg_extract_metadata(project, logger):
    #project.build_depends_on('boto3', '==1.4.7')
    logger.info("I am building extract-metadata for {0}!".format(project.name))
    #project.set_property("dir_source_main_python", "functions/extract-metadata/")
    #project.set_property("dir_source_unittest_python", "functions/extract-metadata/tests/")
    #project.set_property("dir_source_main_scripts", "scripts/")
    #project.set_property("dir_dist", "$dir_target/dist/{0}".format(extract_metadata_path))
    for name, _ in dependencies:
        vendor_path = os.path.join('target/dist/{0}'.format("extract-metadata"), name)
        logger.info("This is the vendor path {0}".format(vendor_path))
        if os.path.isdir(vendor_path):
            continue
        derp_path = os.path.abspath(os.path.dirname(importlib.import_module(name).__file__))
        logger.info("this is derp path {0}".format(derp_path))
        shutil.copytree(derp_path, vendor_path)
    with zipfile.ZipFile('target/dist/serverless-{0}.zip'.format("extract-metadata"), 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('target/dist/{0}'.format("extract-metadata")):
            for file in files:
                zipf.write(os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file), 'target/dist/{0}'.format("extract-metadata")))

@task
@description("Package the function Route Raw for deployment")
def pkg_route_raw(project, logger):
    logger.info("I am building route-raw for {0}!".format(project.name))
    project.set_property("dir_source_main_python", "functions/route-raw/")
    project.set_property("dir_source_unittest_python", "functions/route-raw/tests/")
    project.set_property("dir_source_main_scripts", "scripts/")
    project.set_property("dir_source_main_scripts", "scripts/")
    project.set_property("dir_dist", "$dir_target/dist/{0}".format(route_raw_path))
    for name, _ in dependencies:
        vendor_path = os.path.join('target/dist/{0}'.format(route_raw_path), name)
        if os.path.isdir(vendor_path):
            continue
        dep_path = os.path.abspath(os.path.dirname(importlib.import_module(name).__file__))
        shutil.copytree(dep_path, vendor_path)
    with zipfile.ZipFile('target/dist/serverless-{0}.zip'.format("route-raw"), 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('target/dist/{0}'.format(route_raw_path)):
            for file in files:
                zipf.write(os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file), 'target/dist/{0}'.format(route_raw_path)))

@task
@description("Package the function Start Job Store for deployment")
def pkg_start_job_store(project, logger):
# start-job-store
    logger.info("I am building start_job_store for {0}!".format(project.name))
    project.set_property("dir_source_main_python", "functions/start-job-store/")
    project.set_property("dir_source_unittest_python", "functions/start-job-store/tests/")
    project.set_property("dir_source_main_scripts", "scripts/")
    project.set_property("dir_source_main_scripts", "scripts/")
    project.set_property("dir_dist", "$dir_target/dist/{0}".format(start_job_store_path))
    for name, _ in dependencies:
        vendor_path = os.path.join('target/dist/{0}'.format(start_job_store_path), name)
        if os.path.isdir(vendor_path):
            continue
        dep_path = os.path.abspath(os.path.dirname(importlib.import_module(name).__file__))
        shutil.copytree(dep_path, vendor_path)
    with zipfile.ZipFile('target/dist/serverless-{0}.zip'.format("start-job-store"), 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('target/dist/{0}'.format(start_job_store_path)):
            for file in files:
                zipf.write(os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file), 'target/dist/{0}'.format(start_job_store_path)))

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