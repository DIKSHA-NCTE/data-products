#   -*- coding: utf-8 -*-
from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.install_dependencies")
use_plugin("python.distutils")


name = "diksha-dataproducts-scheduled-jobs"
default_task = "publish"
version = "5.0.0"
license = "MIT License"


@init
def initialize(project):
    project.depends_on_requirements("requirements.txt")
