import os
import sys
import time

import shortuuid
from yellowdog_client import PlatformClient
from yellowdog_client.model import (
    ApiKey,
    ServicesSchema,
    TaskStatus,
)


class RayDog:
    @classmethod
    def fatal_error(cls, msg):
        print("ERROR", msg)
        sys.exit(-1)

    def __init__(self):
        self.namespace = "raydog"
        self.workertags = ["raydog"]

        # read the configuration from the env variables
        self.verbose = True if os.getenv("VERBOSE") else False

        self.api_url = os.getenv("YD_API_URL")
        self.api_key_id = os.getenv("YD_API_KEY_ID")
        self.api_key_secret = os.getenv("YD_API_KEY_SECRET")

        self.ray_port = os.getenv("YD_RAY_PORT", "6379")

        # if there is a cluster id in the environment, use it
        self.clusterid = os.getenv("YD_CLUSTER_ID")
        if not self.clusterid:
            # otherwise create one
            shortuuid.set_alphabet("0123456789abcdefghijklmnopqrstuvwxyz")
            self.clusterid = shortuuid.uuid()
            os.environ["YD_CLUSTER_ID"] = self.clusterid

        # connect to YellowDog
        self.ydclient = PlatformClient.create(
            ServicesSchema(defaultUrl=self.api_url),
            ApiKey(self.api_key_id, self.api_key_secret),
        )
        self.ydworkapi = self.ydclient.work_client

        self.headnode = None
        self.workers = []

    def _get_wr_name(self):
        return "wr-" + self.clusterid

    def _get_head_task_group(self):
        return self.workreq.taskGroups[0]

    def _get_worker_task_group(self):
        return self.workreq.taskGroups[1]

    def _get_ip_address(self, workerid):
        """
        Derive the node ID from the worker ID, and return
        the public, private IP addresses.
        """
        try:
            node = self.ydclient.worker_pool_client.get_node_by_id(
                workerid.replace("wrkr", "node")[:-2]
            )
            return node.details.publicIpAddress, node.details.privateIpAddress
        except:
            RayDog.fatal_error(f"Failed to find host for YD worker {workerid}")

    def _wait_for_task(self, taskid):
        print(f"Waiting for {taskid} to start")
        while True:
            info = self.ydworkapi.get_task_by_id(taskid)
            print(f"Task status: {info.status}")
            if info.status in [TaskStatus.EXECUTING]:
                # TODO ... think about all the other Task statuses
                return info
            # print("Waiting for node to start")
            time.sleep(5)
