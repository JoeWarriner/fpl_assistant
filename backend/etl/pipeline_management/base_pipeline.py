from etl.pipeline_management.serializers import PipelineTaskSerializer, DAGTopologicalSerializer
from etl.jobs.base_job import Job
from etl.utils.logging import log


class Pipeline(Job):
    expects_input = False
    task_serializer: type[PipelineTaskSerializer] = DAGTopologicalSerializer

    def __init__(self):
        self.tasks: dict[Job, set[Job]] = {}         
        
    @property
    def task_list(self):
        return self.tasks.keys()
    
    def add_task(self, task: Job, predecessors: set[Job] = set()):
        self.tasks[task] = predecessors


    def run(self):
        
        serializer = self.task_serializer(self.tasks)
        log('Starting pipeline, serlialising tasks')
        sorted_task_list = serializer.serialize()
        log(f'Task list: {sorted_task_list}')
        data = None
        for task in sorted_task_list:
            if task.expects_input: 
                data = task.run(data)
            else:
                data = task.run()
            