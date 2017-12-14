"""This module was for pybuilder and might be removed soon."""

from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.distutils")

# https://github.com/pybuilder/pybuilder/issues/245
name = "gs.app.s3-trigger-lambda"
default_task = "publish"

@init
def set_properties(project):
    project.set_property("dir_source_main_python", "functions/")
    project.set_property("dir_source_unittest_python", "tests/")
    project.set_property("dir_source_main_scripts", "scripts/")