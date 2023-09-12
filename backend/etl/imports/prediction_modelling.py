from etl.jobs.base_job import Job


class ModelPredictions(Job):
    window_size = 10

    def run(self):
        pass