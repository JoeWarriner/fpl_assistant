from etl.pipeline_management.serializers import JobSerializer, TopologicalSerializer, JobDependencyMapping
from etl.jobs.base_job import Job
from etl.utils.logging import log



class Pipeline(Job):
    """
    Pipeline class that aggregates and serializers data processing jobs.
    """
    expects_input = False
    serializer: type[JobSerializer] = TopologicalSerializer
    jobs: JobDependencyMapping

    def __init__(self):
        self.jobs: JobDependencyMapping = {}         
        
    @property
    def job_list(self):
        return self.jobs.keys()
    
    def add_job(self, job: Job, predecessors: set[Job] = set()):
        self.jobs[job] = predecessors


    def run(self):
        
        serializer = self.serializer()
        log('Starting pipeline, serlialising tasks')
        sorted_job_list = serializer.serialize(self.jobs)
        log(f'Task list: {sorted_job_list}')
        data = None
        for job in sorted_job_list:
            if job.expects_input: 
                data = job.run(data)
            else:
                data = job.run()
            