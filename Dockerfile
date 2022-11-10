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
########################################################################################
# This Dockerfile is intented for devops testing and deb package generation
#
# To run stage 2 locally:
#
#   docker build -t stage2 .
#   docker run -ti -v `pwd`:/work -w /work --entrypoint /bin/bash stage2
#   devops-stages/stage-test.sh
#   devops-stages/stage-build.sh
#

FROM ubuntu:20.04

ARG APT_PROXY
RUN if [ ! -z $APT_PROXY ] ; then \
    echo "Acquire::http::Proxy \"$APT_PROXY\";" > /etc/apt/apt.conf.d/proxy.conf ;\
    echo "Acquire::https::Proxy \"$APT_PROXY\";" >> /etc/apt/apt.conf.d/proxy.conf ;\
    fi

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
        debhelper \
        dh-python \
        git \
        python3 \
        python3-all \
        python3-dev \
        python3-setuptools

RUN python3 -m easy_install pip==22.3
RUN pip install tox==3.24.5

