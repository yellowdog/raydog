from yellowdog_client.model import Task, TaskStatus, TaskSearch

from raydog import RayDog

class RayDogHead(RayDog):
    def __init__(self):
        # most setup is done in the base class
        super().__init__() 

        # keep a count of workers
        self.workerid = 0

        # find the right YellowDog work requirement 
        self.workreq = self.ydworkapi.get_work_requirement_by_name(self.namespace, self._get_wr_name())

        # find the task for the head node
        tasks = self.ydworkapi.get_tasks(
            TaskSearch(
                workRequirementId=self.workreq.id,
                taskGroupId=self._get_head_task_group().id,
                name="ray-head",
                statuses=[ TaskStatus.EXECUTING ]
            )
        ).iterate()

        # wait for the head node to start 
        # ... just in case this code isn't running on the head node
        headid = next(tasks).id
        headtask = self._wait_for_task(headid)

        # get the head node's IP address
        self.publicip, self.privateip = self._get_ip_address(headtask.workerId)
        print("Head node IP private address:", self.privateip)


    def add_workers(self, howmany=1):
        print("Requesting", howmany, "more worker nodes")

        # add task for the new Ray workers
        tasklist = []
        for n in range(0, howmany):
            self.workerid += 1
            tasklist.append(Task(
                name = self._get_worker_name(self.workerid),
                taskType = "ray-worker",
                environment = {
                    "YD_RAY_HEAD_NODE" : self.privateip,
                    "YD_RAY_PORT":       self.ray_port,
                    "YD_CLUSTER_ID" :    self.clusterid
                }
            ))
            
        # get YellowDog to assign then to nodes
        newtasks = self.ydworkapi.add_tasks_to_task_group(
            self._get_worker_task_group(), 
            tasklist)

        # wait for all the workers to start up
        for thetask in newtasks:
            print(f"Waiting for {thetask.name} to start")
            self._wait_for_task(thetask.id)

    def _get_worker_name(self, n):
        return "ray-worker-" + str(n)

    def del_workers(self):
        pass
