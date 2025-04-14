import dotenv

from yellowdog_client.model import WorkRequirement, RunSpecification, Task, TaskGroup
from raydog import RayDog

class RayDogClient(RayDog):
    def __init__(self):
        # read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        self.workreq = None

        # most setup is done in the base class
        super().__init__() 

    def start_head_node(self):
        print("Telling YellowDog to create head node")

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
        self.workreq = self.ydworkapi.add_work_requirement(workreq)

        # add a task for the head node
        headtask = Task(
            name = "ray-head",
            taskType = "ray-head",
            environment = {
                "YD_API_URL" :        self.api_url, 
                "YD_API_KEY_ID" :     self.api_key_id,
                "YD_API_KEY_SECRET" : self.api_key_secret,
                "YD_CLUSTER_ID" :     self.clusterid
            }
        )
        newtasks = self.ydworkapi.add_tasks_to_task_group(self._get_head_task_group(), [headtask])

        # wait for the head node to start
        headid = newtasks[0].id
        headtask = self._wait_for_task(headid)

        # and read it's IP address
        headip = self._get_ip_address(headtask.workerId)
        print("Head node IP address:", headip)

        return headip

    def shutdown(self):
        # cancel the work requirement and abort all tasks
        print("Shutting down the Ray cluster")
        if self.workreq:
            self.ydworkapi.cancel_work_requirement_by_id(self.workreq.id, abort=True)
