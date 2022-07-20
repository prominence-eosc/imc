import json
import htcondor

def get_json(iwd):
    """
    Get the JSON job description
    """
    job = {}
    try:
        with open(job['Iwd'] + '/.job.json') as json_file:
            job = json.load(json_file)
    except:
        pass

    return job

def get_idle_jobs():
    """
    Get the number of idle jobs
    """
    constraint = 'JobStatus == 1 && ProminenceType == "Job" && ProminenceWantJobRouter =!= True && CurrentTime - QDate > 60'

    coll = htcondor.Collector()
    schedds = coll.query(htcondor.AdTypes.Schedd, "true", ["Name"])

    idle_jobs = []
    for schedd in schedds:
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, schedd["Name"])
        jobs = htcondor.Schedd(schedd_ad).query(constraint,
                                                ['ClusterId',
                                                 'ProminenceIdentity',
                                                 'ProminenceGroup',
                                                 'ProminenceMaxRunTime',
                                                 'ProminenceAutoScalingType',
                                                 'RequestCpus',
                                                 'RequestMemory',
                                                 'RequestDisk',
                                                 'QDate',
                                                 'MinHosts',
                                                 'Requirements',
                                                 'Iwd'])

        for job in jobs:
            if 'MinHosts' not in job:
                job['MinHosts'] = 1
            idle_jobs.append({'cpus': int(job['RequestCpus']),
                              'memory': int(job['RequestMemory']),
                              'disk': int(job['RequestDisk']),
                              'nodes': int(job['MinHosts']),
                              'walltime': int(job['ProminenceMaxRunTime']),
                              'identity': job['ProminenceIdentity'],
                              'group': job['ProminenceGroup'],
                              'autoscalingtype': job['ProminenceAutoScalingType'],
                              'created': job['QDate'],
                              'id': int(job['ClusterId']),
                              'iwd': job['Iwd']})
    return idle_jobs

