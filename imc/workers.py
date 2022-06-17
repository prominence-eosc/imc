import htcondor

def get_workers():
    """
    Get list of workers
    """
    coll = htcondor.Collector()

    results = coll.query(htcondor.AdTypes.Startd,
                         'PartitionableSlot=?=True',
                         ['Machine',
                          'NumDynamicSlots',
                          'ProminenceCloud',
                          'ProminenceInfrastructureId',
                          'ProminenceUniqueInfrastructureId'])

    workers = []
    for result in results:
        if 'ProminenceInfrastructureId' in result:
            workers.append({'name': result['Machine'],
                            'id': result['ProminenceInfrastructureId'],
                            'unique_id': result['ProminenceUniqueInfrastructureId'],
                            'site': result['ProminenceCloud'],
                            'slots_in_use': int(result['NumDynamicSlots'])})

    return workers

