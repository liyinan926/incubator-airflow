# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from kubernetes import client, config

class SparkKubernetesHook(BaseHook, LoggingMixin):
    """
    This hook works with the Spark Operator, i.e., it submits Spark applications to run by creating user-specified
    SparkApplication CustomResourceDefinition (CRD) objects to the Kubernetes API server of a Kubernetes cluster.
    User-specified SparkApplication CRD objects are passed in this hook.
    """

    def __init__(self,
                 in_cluster=False,
                 namespace="default",
                 name="",
                 crd_object=None):
        self._in_cluster = in_cluster
        self._namespace = namespace
        self._name = name
        self._crd_object = crd_object
        self._crd_client = self.get_kubernetes_crd_client()

    def create(self):
        self.log.info("Creating SparkApplication object %s", self._name)
        resp = self._crd_client.create_namespaced_custom_object(group="spark-operator.k8s.io",
                                                                version="v1alpha1",
                                                                namespace=self._namespace,
                                                                plural="sparkapplications",
                                                                body=self._crd_object)
        self.log.info("Status: %s", str(resp.status))

    def delete(self):
        self.log.info("Deleting SparkApplication object %s", self._name)
        resp = self._crd_client.delete_namespaced_custom_object(group="spark-operator.k8s.io",
                                                                version="v1alpha1",
                                                                namespace=self._namespace,
                                                                plural="sparkapplications",
                                                                name=self._name,
                                                                body=client.V1DeleteOptions())
        self.log.info("Status: %s", str(resp.status))

    def get_conn(self):
        pass

    def get_kubernetes_crd_client(self):
        if self._in_cluster:
            config.load_incluster_config()
        config.load_kube_config()
        return client.CustomObjectsApi()
