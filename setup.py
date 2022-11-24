#!/usr/bin/env python3
#
# Copyright ETSI Contributors and Others.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from setuptools import setup, find_packages, find_namespace_packages

_name = "osm_ngsa"
_description = "OSM Service Assurance Airflow DAGs and libraries"
with open(os.path.join(".", "README.rst")) as readme_file:
    README = readme_file.read()

setup(
    name=_name,
    description=_description,
    long_description=README,
    use_scm_version={
        "write_to": "src/osm_ngsa/_version.py"
    },
    author="ETSI OSM",
    author_email="osmsupport@etsi.org",
    maintainer="ETSI OSM",
    maintainer_email="osmsupport@etsi.org",
    url="https://osm.etsi.org/gitweb/?p=osm/NG-SA.git;a=summary",
    license="Apache 2.0",
    package_dir={"": "src"},
    packages=find_namespace_packages(where='src'),
    include_package_data=True,
    setup_requires=["setuptools-scm"],
)

