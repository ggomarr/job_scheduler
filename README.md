# job_scheduler
This class helps process jobs in batches in a way that does not impact other more important tasks.

The class is independent from but has many sinergies with the parallelizer function. The example contained in the job_scheduler code file demonstrates this sinergy, and imports the parallelizer (although the class itself would work without it). The parallelizer can be found in a separate repository, and must be made available for import for the job scheduler to load without problems. Manually adding the parallelizer to the code (or removing the example altogether) would also work.

The job_scheduler takes the following parameters:
- jobs_file: a file containing the jobs to be launched
- jobs_dict: a dictionary linking function names to actual functions
- sleep_schedule=[ [datetime.time(22), datetime.time(4)] ]: a list containing ranges of times when the job_scheduler is not to launch any new job
- check_for=['production_job','pause']: a list containing the operations that should keep the job_scheduler from launching new jobs
- check_every=300: if the scheduler is waiting for an operation to finish, how often it should check in seconds

Each job to be launched is a line in a jobs file. The syntax of the lines is the following:
```
launched_flag, finished_flag, function_name, kwarg_1_type kwarg_1_name kwarg_1, kwarg_2_type kwarg_2_name kwarg_2, ...
```
An example of a job would be:
```
pending, unfinished, sample_parallelizer_job, str task_param_lst_file sample_param_lst_001.txt, eval job_param datetime.date.today()
```
Whenever a job is launched, the job_scheduler replaces the 'pending' launch_flag with the start time and saves the jobs file. An example of a launched but unfinished job would look like this:
```
2017-05-16 12:56:00, unfinished, sample_parallelizer_job, str task_param_lst_file sample_param_lst_001.txt, eval job_param datetime.date.today()
```
Whenever a job is finished, the job_scheduler replaces the 'unfinished' finished_flag with the end time and saves the jobs file. An example of a finished job would look like this:
```
2017-05-16 12:56:00, 2017-05-16 12:56:41, sample_parallelizer_job, str task_param_lst_file sample_param_lst_001.txt, eval job_param datetime.date.today()
```
The function_name should be linked to an actual function through the jobs_dict dictionary.

The job_scheduler will launch jobs sequentially, but only if:
- The time of the day is not within the sleep_schedule, and
- None of the production jobs contained in check_for are running

The job_scheduler will think that an operation is running if it can find a file with the name of the operation in question. For example, if check_for contains the string 'pause', creating an empty file with the name 'pause' will force the job_scheduler to stop launching new jobs until the 'pause' file is removed.

An interrupted job file can be reused, as long as the jobs themselves are either finished or pending:
```
[...]
2017-05-16 12:56:00, 2017-05-16 12:56:41, sample_parallelizer_job, str task_param_lst_file sample_param_lst_023.txt, eval job_param datetime.date.today()
pending, unifinished, sample_parallelizer_job, str task_param_lst_file sample_param_lst_024.txt, eval job_param datetime.date.today()
[...]
```
If a job was interrupted half the way through its execution, the job scheduler will ask the user to reset the job's launched_flag to 'pending' first.

Running the file directly from the command line will create a job_scheduler tasked with executing two parallelizer jobs, which make their threads sleep and then print some messages. The log of the execution of those jobs should look as follows:
```
05/16/2017 01:16:14 PM process_jobs INFO     Launching jobs in sample_jobs_file.txt...
05/16/2017 01:16:14 PM process_jobs INFO     Processing Job 1/2 (sample_parallelizer_job)...
05/16/2017 01:16:14 PM sample_parallelizer_job INFO     Parallelizing 13 subjobs...
05/16/2017 01:16:17 PM sample_task  INFO     [02][13:16:14 Slept 3s] Mary also had a gigantic crocodile
05/16/2017 01:16:17 PM sample_task  INFO     [01][13:16:14 Slept 3s] Mary had a little lamb
05/16/2017 01:16:20 PM sample_task  INFO     [03][13:16:14 Slept 3s] The crocodile was large and green and very ferocious
05/16/2017 01:16:20 PM sample_task  INFO     [04][13:16:14 Slept 3s] The lamb was sheepishly looking
05/16/2017 01:16:20 PM timer        INFO     4 items processed [0:00:06]. 9 items left [0:00:14]
05/16/2017 01:16:23 PM sample_task  INFO     [05][13:16:14 Slept 3s] Mary kept both at an undisclosed location near Area 51
05/16/2017 01:16:23 PM sample_task  INFO     [06][13:16:14 Slept 3s] Mary did not keep the two separated by wall or fence of any kind
05/16/2017 01:16:25 PM timer        INFO     6 items processed [0:00:11]. 7 items left [0:00:13]
05/16/2017 01:16:26 PM sample_task  INFO     [07][13:16:14 Slept 3s] The crocodile was walking in circles around a tiny puddle
05/16/2017 01:16:26 PM sample_task  INFO     [08][13:16:14 Slept 3s] when it caugh a glimpse of the sheepishly looking lamb
05/16/2017 01:16:29 PM sample_task  INFO     [09][13:16:14 Slept 3s] on the corner of its shiny eye
05/16/2017 01:16:29 PM sample_task  INFO     [10][13:16:14 Slept 3s] The ferocious crocodile immediately and happily thought:
05/16/2017 01:16:30 PM timer        INFO     10 items processed [0:00:16]. 3 items left [0:00:04]
05/16/2017 01:16:32 PM sample_task  INFO     [11][13:16:14 Slept 3s] "Yum! Dinner is served!"
05/16/2017 01:16:32 PM sample_task  INFO     [12][13:16:14 Slept 3s] The vicious crocodile threw its 2000 pounds of flesh towards the lamb
05/16/2017 01:16:35 PM sample_task  INFO     [13][13:16:14 Slept 3s] tail wiggling and teeth sharp as chirurgical blades
05/16/2017 01:16:35 PM sample_parallelizer_job INFO     13 (out of 13) tasks successfully completed!
05/16/2017 01:16:35 PM process_jobs INFO     Done with Job 1/2 (sample_parallelizer_job) in 21.44!
05/16/2017 01:16:35 PM process_jobs INFO     Processing Job 2/2 (sample_parallelizer_job)...
05/16/2017 01:16:35 PM sample_parallelizer_job INFO     Parallelizing 13 subjobs...
05/16/2017 01:16:38 PM sample_task  INFO     [01][13:16:35 Slept 3s] The lamb was not afraid
05/16/2017 01:16:38 PM sample_task  INFO     [02][13:16:35 Slept 3s] for the lamb was a kung-fu master and a part-time hacker in disguise
05/16/2017 01:16:41 PM timer        INFO     2 items processed [0:00:05]. 11 items left [0:00:30]
05/16/2017 01:16:41 PM sample_task  INFO     [03][13:16:35 Slept 3s] The lamb stopped the incoming train of flesh
05/16/2017 01:16:41 PM sample_task  INFO     [04][13:16:35 Slept 3s] with a perfectly delivered jump-kick to the crocodile's jaw
05/16/2017 01:16:44 PM sample_task  INFO     [05][13:16:35 Slept 3s] to then hack into the crocodile's mainframe
05/16/2017 01:16:44 PM sample_task  INFO     [06][13:16:35 Slept 3s] through the unprotected ethernet port on the back of its head
05/16/2017 01:16:46 PM timer        INFO     6 items processed [0:00:10]. 7 items left [0:00:12]
05/16/2017 01:16:47 PM sample_task  INFO     [07][13:16:35 Slept 3s] where it installed an elaborate computer virus
05/16/2017 01:16:47 PM sample_task  INFO     [08][13:16:35 Slept 3s] The virus to this day
05/16/2017 01:16:50 PM sample_task  INFO     [09][13:16:35 Slept 3s] keeps the crocodile walking around
05/16/2017 01:16:50 PM sample_task  INFO     [10][13:16:35 Slept 3s] looking significantly less ferocious
05/16/2017 01:16:51 PM timer        INFO     10 items processed [0:00:15]. 3 items left [0:00:04]
05/16/2017 01:16:53 PM sample_task  INFO     [11][13:16:35 Slept 3s] and lighting up every 3 minutes
05/16/2017 01:16:53 PM sample_task  INFO     [12][13:16:35 Slept 3s] like a Christmas tree illuminating the sky
05/16/2017 01:16:55 PM timer        INFO     12 items processed [0:00:20]. 1 items left [0:00:01]
05/16/2017 01:16:56 PM sample_task  INFO     [13][13:16:35 Slept 3s] with the faintness of an autumn breeze
05/16/2017 01:16:57 PM sample_parallelizer_job INFO     13 (out of 13) tasks successfully completed!
05/16/2017 01:16:57 PM process_jobs INFO     Done with Job 2/2 (sample_parallelizer_job) in 21.99!
```
Enjoy, and do let me know if you find this useful!