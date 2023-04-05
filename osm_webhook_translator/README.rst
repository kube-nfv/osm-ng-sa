..
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

==================
webhook-translator
==================

webhook-translator is a component in the Service Assurance architecture for OSM.

Its role is to receive alerts from entities such as Prometheus AlertManager or external systems, and to translate them to a format that can be consumed by Airflow DAGs. It basically receives HTTP POST messages and forwards them to an Airflow webhook

The main characteristics are:

* Lightweight: a very small number of lines of code does the work.
* Stateless. It only translates HTTP requests. No state for those translations. When running as a Kubernetes deployment, native scaling is achieved by means of Kubernetes services.
* Simple. Based on `FastAPI <https://fastapi.tiangolo.com/>`
* Independent from the source of the alert. No maintenance is required to incorporate new alert sources.

