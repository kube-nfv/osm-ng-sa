# syntax=docker/dockerfile:1
#######################################################################################
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
#######################################################################################


##########################
### Stage 1: Build Stage #
##########################

FROM apache/airflow:2.5.3-python3.10 AS build

ENV PIP_DISABLE_PIP_VERSION_CHECK=1

ARG COMMON_REFSPEC=master

USER root

# Install only essential build dependencies
RUN apt-get update && \
      apt-get install -y --no-install-recommends \
      gcc git python3-dev && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Install base requirements
RUN pip install --no-cache-dir setuptools-scm wheel

# Install OSM common from Git
RUN git clone --filter=blob:none --tags https://osm.etsi.org/gitlab/osm/common.git /tmp/osm-common && \
  cd /tmp/osm-common && \
  git fetch origin ${COMMON_REFSPEC} && \
  git checkout FETCH_HEAD && \
  cd - && \
  pip3 wheel --no-cache-dir -r /tmp/osm-common/requirements.txt -w /tmp/wheels && \
  pip3 wheel --no-cache-dir /tmp/osm-common -w /tmp/wheels

# Install NG_SA requirements
COPY requirements.txt /tmp/osm-ngsa/requirements.txt
RUN pip wheel --no-cache-dir -r /tmp/osm-ngsa/requirements.txt -w /tmp/wheels

# Install NG_SA
COPY . /tmp/osm-ngsa
USER root
# workaound to allow the airflow user to write files in the copied folder
RUN chown -R airflow:root /tmp/osm-ngsa && \
  chmod -R u+w /tmp/osm-ngsa
USER airflow
RUN pip wheel --no-cache-dir /tmp/osm-ngsa -w /tmp/wheels


#########################################################################################################################################################################

################################
# Stage 2: Runtime environment #
################################

FROM apache/airflow:2.5.3-python3.10

USER airflow

# Copy all artifacts to install
COPY --chown=airflow:root --from=build /tmp/wheels /tmp/wheels
COPY --chown=airflow:root --from=build /tmp/osm-common/requirements.txt /tmp/osm-requirements.txt
COPY --chown=airflow:root --from=build /tmp/osm-ngsa/requirements.txt /tmp/ngsa-requirements.txt

RUN pip install --no-cache-dir --no-index --find-links=/tmp/wheels \
    -r /tmp/osm-requirements.txt \
    -r /tmp/ngsa-requirements.txt \
    osm_common \
    osm_ngsa && \
  rm -rf /tmp/wheels

