import os
import time
import logging
import datetime

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
    import parallelizer
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
            task_params=[ task_param.strip() for task_param in task_params.split(',') ]
            step_id=int(task_params[0])
            sleep_time=int(task_params[1])
            msg=task_params[2]
            time.sleep(sleep_time)
            logger.info('[{:02d}][{} Slept {}s] {}'.format(step_id,job_param.strftime('%H:%M:%S'),sleep_time,msg))
            return True
        except:
            return False
    def sample_parallelizer_job(task_param_lst_file,job_param,log_level=logging.DEBUG):
        logger=logging.getLogger('sample_parallelizer_job')
        logger.setLevel(log_level)
        with open(task_param_lst_file,'r') as f:
            task_params_lst=f.readlines()
        num_tasks=len(task_params_lst)
        logger.info('Parallelizing {} subjobs...'.format(num_tasks))
        processed_tasks=parallelizer.parallelize(parallelizer.function_wrapper,task_params_lst,
                                                 (sample_task,),job_param=job_param,
                                                 cores=2,timer_step=5,max_time_stuck=10)
        logger.info('{} (out of {}) tasks successfully completed!'.format(sum(processed_tasks),num_tasks))
    sample_jobs_dict={
        'sample_parallelizer_job': sample_parallelizer_job,
    }
# sample_jobs_file.txt
# pending, unfinished, sample_parallelizer_job, str task_param_lst_file sample_param_lst_001.txt, eval job_param datetime.datetime.now()
# pending, unfinished, sample_parallelizer_job, str task_param_lst_file sample_param_lst_002.txt, eval job_param datetime.datetime.now()
# sample_param_lst_001.txt
#  1, 3, Mary had a little lamb
#  2, 3, Mary also had a gigantic crocodile
#  3, 3, The crocodile was large and green and very ferocious
#  4, 3, The lamb was sheepishly looking
#  5, 3, Mary kept both at an undisclosed location near Area 51
#  6, 3, Mary did not keep the two separated by wall or fence of any kind
#  7, 3, The crocodile was walking in circles around a tiny puddle
#  8, 3, when it caugh a glimpse of the sheepishly looking lamb
#  9, 3, on the corner of its shiny eye
# 10, 3, The ferocious crocodile immediately and happily thought:,
# 11, 3, "Yum! Dinner is served!"
# 12, 3, The vicious crocodile threw its 2000 pounds of flesh towards the lamb,
# 13, 3, tail wiggling and teeth sharp as chirurgical blades
# sample_param_lst_002.txt
#  1, 3, The lamb was not afraid
#  2, 3, for the lamb was a kung-fu master and a part-time hacker in disguise
#  3, 3, The lamb stopped the incoming train of flesh
#  4, 3, with a perfectly delivered jump-kick to the crocodile's jaw
#  5, 3, to then hack into the crocodile's mainframe
#  6, 3, through the unprotected ethernet port on the back of its head
#  7, 3, where it installed an elaborate computer virus
#  8, 3, The virus to this day
#  9, 3, keeps the crocodile walking around
# 10, 3, looking significantly less ferocious
# 11, 3, and lighting up every 3 minutes
# 12, 3, like a Christmas tree illuminating the sky
# 13, 3, with the faintness of an autumn breeze
    scheduler=job_scheduler('sample_jobs_file.txt',sample_jobs_dict,
                            sleep_schedule=[
                                            [datetime.time(22),datetime.time(4)],
                                           ],
                            check_for=['production_job','pause'],check_every=5)
    scheduler.process_jobs()
# Expected output
# 05/16/2017 01:16:14 PM process_jobs INFO     Launching jobs in sample_jobs_file.txt...
# 05/16/2017 01:16:14 PM process_jobs INFO     Processing Job 1/2 (sample_parallelizer_job)...
# 05/16/2017 01:16:14 PM sample_parallelizer_job INFO     Parallelizing 13 subjobs...
# 05/16/2017 01:16:17 PM sample_task  INFO     [02][13:16:14 Slept 3s] Mary also had a gigantic crocodile
# 05/16/2017 01:16:17 PM sample_task  INFO     [01][13:16:14 Slept 3s] Mary had a little lamb
# 05/16/2017 01:16:20 PM sample_task  INFO     [03][13:16:14 Slept 3s] The crocodile was large and green and very ferocious
# 05/16/2017 01:16:20 PM sample_task  INFO     [04][13:16:14 Slept 3s] The lamb was sheepishly looking
# 05/16/2017 01:16:20 PM timer        INFO     4 items processed [0:00:06]. 9 items left [0:00:14]
# 05/16/2017 01:16:23 PM sample_task  INFO     [05][13:16:14 Slept 3s] Mary kept both at an undisclosed location near Area 51
# 05/16/2017 01:16:23 PM sample_task  INFO     [06][13:16:14 Slept 3s] Mary did not keep the two separated by wall or fence of any kind
# 05/16/2017 01:16:25 PM timer        INFO     6 items processed [0:00:11]. 7 items left [0:00:13]
# 05/16/2017 01:16:26 PM sample_task  INFO     [07][13:16:14 Slept 3s] The crocodile was walking in circles around a tiny puddle
# 05/16/2017 01:16:26 PM sample_task  INFO     [08][13:16:14 Slept 3s] when it caugh a glimpse of the sheepishly looking lamb
# 05/16/2017 01:16:29 PM sample_task  INFO     [09][13:16:14 Slept 3s] on the corner of its shiny eye
# 05/16/2017 01:16:29 PM sample_task  INFO     [10][13:16:14 Slept 3s] The ferocious crocodile immediately and happily thought:
# 05/16/2017 01:16:30 PM timer        INFO     10 items processed [0:00:16]. 3 items left [0:00:04]
# 05/16/2017 01:16:32 PM sample_task  INFO     [11][13:16:14 Slept 3s] "Yum! Dinner is served!"
# 05/16/2017 01:16:32 PM sample_task  INFO     [12][13:16:14 Slept 3s] The vicious crocodile threw its 2000 pounds of flesh towards the lamb
# 05/16/2017 01:16:35 PM sample_task  INFO     [13][13:16:14 Slept 3s] tail wiggling and teeth sharp as chirurgical blades
# 05/16/2017 01:16:35 PM sample_parallelizer_job INFO     13 (out of 13) tasks successfully completed!
# 05/16/2017 01:16:35 PM process_jobs INFO     Done with Job 1/2 (sample_parallelizer_job) in 21.44!
# 05/16/2017 01:16:35 PM process_jobs INFO     Processing Job 2/2 (sample_parallelizer_job)...
# 05/16/2017 01:16:35 PM sample_parallelizer_job INFO     Parallelizing 13 subjobs...
# 05/16/2017 01:16:38 PM sample_task  INFO     [01][13:16:35 Slept 3s] The lamb was not afraid
# 05/16/2017 01:16:38 PM sample_task  INFO     [02][13:16:35 Slept 3s] for the lamb was a kung-fu master and a part-time hacker in disguise
# 05/16/2017 01:16:41 PM timer        INFO     2 items processed [0:00:05]. 11 items left [0:00:30]
# 05/16/2017 01:16:41 PM sample_task  INFO     [03][13:16:35 Slept 3s] The lamb stopped the incoming train of flesh
# 05/16/2017 01:16:41 PM sample_task  INFO     [04][13:16:35 Slept 3s] with a perfectly delivered jump-kick to the crocodile's jaw
# 05/16/2017 01:16:44 PM sample_task  INFO     [05][13:16:35 Slept 3s] to then hack into the crocodile's mainframe
# 05/16/2017 01:16:44 PM sample_task  INFO     [06][13:16:35 Slept 3s] through the unprotected ethernet port on the back of its head
# 05/16/2017 01:16:46 PM timer        INFO     6 items processed [0:00:10]. 7 items left [0:00:12]
# 05/16/2017 01:16:47 PM sample_task  INFO     [07][13:16:35 Slept 3s] where it installed an elaborate computer virus
# 05/16/2017 01:16:47 PM sample_task  INFO     [08][13:16:35 Slept 3s] The virus to this day
# 05/16/2017 01:16:50 PM sample_task  INFO     [09][13:16:35 Slept 3s] keeps the crocodile walking around
# 05/16/2017 01:16:50 PM sample_task  INFO     [10][13:16:35 Slept 3s] looking significantly less ferocious
# 05/16/2017 01:16:51 PM timer        INFO     10 items processed [0:00:15]. 3 items left [0:00:04]
# 05/16/2017 01:16:53 PM sample_task  INFO     [11][13:16:35 Slept 3s] and lighting up every 3 minutes
# 05/16/2017 01:16:53 PM sample_task  INFO     [12][13:16:35 Slept 3s] like a Christmas tree illuminating the sky
# 05/16/2017 01:16:55 PM timer        INFO     12 items processed [0:00:20]. 1 items left [0:00:01]
# 05/16/2017 01:16:56 PM sample_task  INFO     [13][13:16:35 Slept 3s] with the faintness of an autumn breeze
# 05/16/2017 01:16:57 PM sample_parallelizer_job INFO     13 (out of 13) tasks successfully completed!
# 05/16/2017 01:16:57 PM process_jobs INFO     Done with Job 2/2 (sample_parallelizer_job) in 21.99!