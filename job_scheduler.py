import os
import time
import logging
import datetime

import tiny_timer
import parallelizer

class job_scheduler():
    def __init__(self,jobs_file,jobs_dict,
                 sleep_schedule=[
                                 [datetime.time(22),datetime.time(4)],
                                ],
                 check_for=['production_job','pause'],check_every=300):
        self.jobs_file=jobs_file
        self.jobs_dict=jobs_dict
        self.sleep_schedule=sleep_schedule
        self.check_for=check_for
        self.check_every=check_every
        try:
            with open(self.jobs_file,'r') as f:
                self.jobs=[ [s.strip() for s in line.split(', ')] for line in f.readlines()]
        except:
            open(self.jobs_file,'w').close()
            self.jobs=[]
    def export_jobs(self):
        with open(self.jobs_file,'w') as f:
            f.writelines([ ', '.join(job)+'\n' for job in self.jobs])
    def process_jobs(self,archive_jobs=False,log_level=logging.DEBUG):
        logger=logging.getLogger('process_jobs')
        logger.setLevel(log_level)
        logger.info('Launching jobs in {}...'.format(self.jobs_file))
        tot=len(self.jobs)
        for job_pos in range(tot):
            self.check_can_work()
            job_is_pending=self.job_is_pending(job_pos)
            job_is_unfinished=self.job_is_unfinished(job_pos)
            if job_is_pending and job_is_unfinished:
                logger.info('Processing Job {}/{} ({})...'.format(job_pos+1,tot,self.jobs[job_pos][2]))
                start_time=datetime.datetime.now()
                self.jobs[job_pos][0]=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.export_jobs()
                self.execute_job(self.jobs[job_pos])
                self.jobs[job_pos][1]=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.export_jobs()
                elapsed_time=datetime.datetime.now()-start_time
                logger.info('Done with Job {}/{} ({}) in {:0.2f}!'.format(job_pos+1,tot,self.jobs[job_pos][2],
                                                                          elapsed_time.total_seconds()))
            elif job_is_unfinished:
                logger.warning('Job {}/{} ({}) seems to have been interrupted - reset it first!'.format(job_pos+1,tot,self.jobs[job_pos][2]))
                return False
            else:
                logger.info('Job {}/{} ({}) is done already. Moving on...'.format(job_pos+1,tot,self.jobs[job_pos][2]))
        if archive_jobs:
            os.rename(self.jobs_file,'/archive/'.join(os.path.split(self.jobs_file)))
    def check_can_work(self,log_level=logging.DEBUG):
        logger=logging.getLogger('check_can_work')
        logger.setLevel(log_level)
        time_now=datetime.datetime.now()
        for sleep_time in self.sleep_schedule:
            if sleep_time[1]>sleep_time[0]:
                sleep_start=datetime.datetime.combine(datetime.date.today(),sleep_time[0])
                sleep_end=datetime.datetime.combine(datetime.date.today(),sleep_time[1])
            elif time_now.time()>sleep_time[1]:
                sleep_start=datetime.datetime.combine(datetime.date.today(),sleep_time[0])
                sleep_end=datetime.datetime.combine(datetime.date.today()+datetime.timedelta(days=1),
                                                    sleep_time[1])
            else:
                sleep_start=datetime.datetime.combine(datetime.date.today()+datetime.timedelta(days=-1),
                                                      sleep_time[0])
                sleep_end=datetime.datetime.combine(datetime.date.today(),sleep_time[1])
            if time_now>sleep_start and time_now<sleep_end:
                logger.info('It is time to rest (until {})!'.format(sleep_end.strftime('%H:%M:%S')))
                time.sleep((sleep_end-time_now).seconds)
                logger.info('Done resting!')
        while self.check_running_operations():
            logger.info('An important operation is still running. Waiting for {} seconds...'.format(self.check_every))
            time.sleep(self.check_every)
    def check_running_operations(self):
        for operation in self.check_for:
            if os.path.isfile(operation):
                return True
        return False
    def job_is_pending(self,job_pos):
        return self.jobs[job_pos][0]=='pending'
    def job_is_unfinished(self,job_pos):
        return self.jobs[job_pos][1]=='unfinished'
    def extract_params(self,param_lst):
        job_params={}
        for job_param in param_lst:
            job_param=job_param.split()
            if job_param[0]=='str':
                job_params[job_param[1]]=' '.join(job_param[2:])
            elif job_param[0]=='int':
                job_params[job_param[1]]=int(job_param[2])
            elif job_param[0]=='date':
                job_params[job_param[1]]=datetime.datetime.strptime(job_param[2],'%Y-%m-%d').date()
            elif job_param[0]=='list':
                job_params[job_param[1]]=job_param[2:]
            elif job_param[0]=='eval':
                job_params[job_param[1]]=eval(' '.join(job_param[2:]))
        return job_params
    def execute_job(self,job):
        job_type=job[2]
        job_params=self.extract_params(job[3:])
        self.jobs_dict[job_type](**job_params)

if __name__ == '__main__':
    def setup_logger(log_level=logging.DEBUG):
        logger=logging.getLogger()
        formatter=logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s','%m/%d/%Y %I:%M:%S %p')
        handler_stream=logging.StreamHandler()
        handler_stream.setLevel(log_level)
        handler_stream.setFormatter(formatter)
        logger.addHandler(handler_stream)
        return logger
    setup_logger()
    logger=logging.getLogger('sample_run')
    logger.setLevel(logging.DEBUG)
    def sample_task(task_params,job_param,log_level=logging.DEBUG):
        logger=logging.getLogger('sample_task')
        logger.setLevel(log_level)
        try:
            time.sleep(task_params[1])
            logger.debug('[{} {}] Slept for {}s'.format(job_param,task_params[0],task_params[1]))
            return True
        except:
            return False
    def sample_parallelizer_job(task_param_lst_file,job_param,log_level=logging.DEBUG):
        logger=logging.getLogger('sample_parallelizer_job')
        logger.setLevel(log_level)
        with open(task_param_lst_file,'r') as f:
            task_param_lst=[ (p[0].strip(), int(p[1].strip())) for p in [ line.split(',') for line in f.readlines() ] ]
        num_tasks=len(task_param_lst)
        logger.info('Parallelizing {} subjobs...'.format(num_tasks))
        processed_tasks=parallelizer.parallelize(parallelizer.function_wrapper,task_param_lst,
                                                 (sample_task,),job_param=job_param,
                                                 cores=2,timer_step=5,max_time_stuck=10)
        logger.info('{} (out of {}) tasks successfully completed!'.format(sum(processed_tasks),num_tasks))
    sample_jobs_dict={
        'sample_parallelizer_job': sample_parallelizer_job,
    }
# sample_jobs_file.txt
# pending, unfinished, sample_parallelizer_job, str task_param_lst_file sample_param_lst.txt, str job_param Hey
# sample_param_lst.txt
# [01] you, 5
# [02] me, 5
# [03] ho, 5
# [04] let's go, 5
# [05] you, 5
# [06] me, 5
# [07] ho, 5
# [08] let's go, 5
# [09] you, 5
# [10] me, 5
# [11] ho, 5
# [12] let's go, 5
    scheduler=job_scheduler('sample_jobs_file.txt',sample_jobs_dict,
                            sleep_schedule=[
                                            [datetime.time(22),datetime.time(4)],
                                           ],
                            check_for=['production_job','pause'],check_every=5)
    scheduler.process_jobs()
# Expected output
# 05/15/2017 06:12:59 PM process_jobs INFO     Launching jobs in sample_jobs_file.txt...
# 05/15/2017 06:12:59 PM process_jobs INFO     Processing Job 1/1 (sample_parallelizer_job)...
# 05/15/2017 06:12:59 PM sample_parallelizer_job INFO     Parallelizing 12 subjobs...
# 05/15/2017 06:13:04 PM sample_task  DEBUG    [Hey [02] me] Slept for 5s
# 05/15/2017 06:13:04 PM sample_task  DEBUG    [Hey [01] you] Slept for 5s
# 05/15/2017 06:13:05 PM timer        INFO     2 items processed [0:00:06]. 10 items left [0:00:30]
# 05/15/2017 06:13:09 PM sample_task  DEBUG    [Hey [04] let's go] Slept for 5s
# 05/15/2017 06:13:09 PM sample_task  DEBUG    [Hey [03] ho] Slept for 5s
# 05/15/2017 06:13:10 PM timer        INFO     4 items processed [0:00:11]. 8 items left [0:00:22]
# 05/15/2017 06:13:14 PM sample_task  DEBUG    [Hey [05] you] Slept for 5s
# 05/15/2017 06:13:14 PM sample_task  DEBUG    [Hey [06] me] Slept for 5s
# 05/15/2017 06:13:15 PM timer        INFO     6 items processed [0:00:16]. 6 items left [0:00:16]
# 05/15/2017 06:13:19 PM sample_task  DEBUG    [Hey [07] ho] Slept for 5s
# 05/15/2017 06:13:19 PM sample_task  DEBUG    [Hey [08] let's go] Slept for 5s
# 05/15/2017 06:13:20 PM timer        INFO     8 items processed [0:00:21]. 4 items left [0:00:10]
# 05/15/2017 06:13:24 PM sample_task  DEBUG    [Hey [09] you] Slept for 5s
# 05/15/2017 06:13:24 PM sample_task  DEBUG    [Hey [10] me] Slept for 5s
# 05/15/2017 06:13:25 PM timer        INFO     10 items processed [0:00:26]. 2 items left [0:00:05]
# 05/15/2017 06:13:29 PM sample_task  DEBUG    [Hey [11] ho] Slept for 5s
# 05/15/2017 06:13:29 PM sample_task  DEBUG    [Hey [12] let's go] Slept for 5s
# 05/15/2017 06:13:30 PM sample_parallelizer_job INFO     12 (out of 12) tasks successfully completed!
# 05/15/2017 06:13:30 PM process_jobs INFO     Done with Job 1/1 (sample_parallelizer_job) in 31.06!