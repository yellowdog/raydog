import dotenv
import os
import shortuuid
import sys
import time

from dataclasses import dataclass
from typing import Optional

from yellowdog_client import PlatformClient
from yellowdog_client.model import ServicesSchema, ApiKey
from yellowdog_client.model import WorkRequirement, RunSpecification, Task, TaskGroup
from yellowdog_client.model import NodeSearch, NodeStatus, TaskStatus, WorkerStatus

def fatal_error(msg):
    print("ERROR", msg)
    sys.exit(-1)

class RayDog:
    def __init__(self):
        # read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        self.namespace = "raydog"
        self.workertags = [ "raydog", "onpremise-winston" ]

        # read the configuration from the env variables
        self.verbose = True if os.getenv("VERBOSE") else False
        self.api_url = os.getenv("YD_API_URL")
        self.api_key_id = os.getenv("YD_API_KEY_ID")
        self.api_key_secret = os.getenv("YD_API_KEY_SECRET")

        # if there is a cluster id in the environment, use it
        self.clusterid = os.getenv("YD_CLUSTER_ID")
        if not self.clusterid:
            # otherwise create one
            shortuuid.set_alphabet('0123456789abcdefghijklmnopqrstuvwxyz')
            self.clusterid = shortuuid.uuid()
     
        # connect to YellowDog
        self.ydclient = PlatformClient.create(
            ServicesSchema(defaultUrl=self.api_url),
            ApiKey(self.api_key_id, self.api_key_secret)
        )
        self.ydworkapi = self.ydclient.work_client

        self.headnode = None
        self.workers  = []

    def _get_task_env(self):
        return {
            "YD_API_URL" :        self.api_url, 
            "YD_API_KEY_ID" :     self.api_key_id,
            "YD_API_KEY_SECRET" : self.api_key_secret,
            "YD_CLUSTER_ID" :     self.clusterid
        }

    def _get_wr_name(self):
        return "wr-" + self.clusterid
    
    def start_head_node(self):
        # create a work requirement
        workreq = WorkRequirement(
            namespace = self.namespace,
            name = self._get_wr_name(),

            taskGroups = [
                TaskGroup(
                    name = "head-tg-" + self.clusterid,
                    runSpecification=RunSpecification(
                        taskTypes=["ray-head"],
                        maxWorkers=1,
                        maximumTaskRetries=5,
                        workerTags=self.workertags
                    )
                ),
                TaskGroup(
                    name = "worker-tg-" + self.clusterid,
                    runSpecification=RunSpecification(
                        taskTypes=["ray-worker"],
                        maximumTaskRetries=5,
                        workerTags=self.workertags
                    )
                )
           ]
        )
        workreq = self.ydworkapi.add_work_requirement(workreq)

        # add a task for the head node
        headtask = Task(
            name = "rayheadtask",
            taskType = "ray-head",
            environment = self._get_task_env()
        )
        newtasks = self.ydworkapi.add_tasks_to_task_group(workreq.taskGroups[0], [headtask])

        # wait for the head node to start
        headid = newtasks[0].id
        headtask = self._wait_for_task(headid)

        # and read it's IP address
        headip = self._get_ip_address(headtask.workerId)
        print("head node IP address:", headip)

    def _get_ip_address(self, workerid):
        # find the all the nodes that could be hosting the worker
        candidates = self.ydclient.worker_pool_client.find_nodes(NodeSearch(
            statuses=[ NodeStatus.RUNNING ],
            workerStatuses=[ WorkerStatus.DOING_TASK ]
            # TODO: find some other parameters that will narrow the search
        ))

        # look for the one that we need
        for node in candidates:
            for worker in node.workers:
                if worker.id == workerid:
                    return node.details.publicIpAddress
                                
        fatal_error("Failed to find host of worker", workerid)

    def _wait_for_task(self, taskid):
        while True:
            info = self.ydworkapi.get_task_by_id(taskid)
            print("Waiting for head node to start")
            if info.status in [ TaskStatus.EXECUTING ]:
                # TODO ... think about all the other Task statuses
                return info
            time.sleep(2)

    def shutdown(self):
        # shutdown all the worker nodes

        # shutdown the head node

        # cancel the work requireemnt
        self.ydworkapi.cancel_work_requirement_by_id(self.workreq.id)

