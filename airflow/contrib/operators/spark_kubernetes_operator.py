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

from airflow.contrib.hooks.spark_kubernetes_hook import SparkKubernetesHook
from airflow.models import BaseOperator

class SparkKubernetesOperator(BaseOperator):
    """
    This operator works with the Spark Operator, i.e., it submits Spark applications to run by creating user-specified
    SparkApplication CustomResourceDefinition (CRD) objects to the Kubernetes API server of a Kubernetes cluster.
    User-specified SparkApplication CRD objects are passed in this hook.

    :param in_cluster: A flag indicating if the operator is running inside a Kubernetes cluster.
    :type in_cluster: bool
    :param namespace: The namespace of the SparkApplication CRD object.
    :type namespace: string
    :param name: The name of the SparkApplication CRD object.
    :type name: string
    :param crd_object: The name of the SparkApplication CRD object.
    :type crd_object: any
    """

    template_fields = ("namespace", "name", "crd_object")

    def __init__(self,
                 in_cluster=False,
                 namespace="default",
                 name="",
                 crd_object=None,
                 *args,
                 **kwargs):
        super(SparkKubernetesOperator, self).__init__(*args, **kwargs)
        self._hook = SparkKubernetesHook(in_cluster, namespace, name, crd_object)

    def execute(self, context):
        self._hook.create()

    def on_kill(self):
        self._hook.delete()
