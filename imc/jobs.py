import htcondor

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
                              'identity': job['ProminenceIdentity'],
                              'group': job['ProminenceGroup'],
                              'created': job['QDate'],
                              'id': int(job['ClusterId']),
                              'iwd': job['Iwd']})
    return idle_jobs

