"""Determine which resources a job is allowed to run on"""
import logging

# Logging
logger = logging.getLogger(__name__)

class PolicyEngine():
    """
    Policy engine
    """
    def __init__(self, config, job, db, identity):
        self._config = {}
        self._job = job
        self._db = db
        self._identity = identity

        self._clouds = []
        for cloud in config:
            self._clouds.append(cloud['name'])
            self._config[cloud['name']] = cloud

    def get_flavour(self, cloud):
        """
        Return the flavour matching the job
        """
        flavour_name = None
        required_cores = 0
        required_memory = 0
        required_disk = 0

        if 'resources' not in self._job['requirements']:
            return flavour_name

        if 'cores' in self._job['requirements']['resources']:
            required_cores = self._job['requirements']['resources']['cores']
        if 'memory' in self._job['requirements']['resources']:
            required_memory = self._job['requirements']['resources']['memory']
        if 'disk' in self._job['requirements']['resources']:
            required_disk = self._job['requirements']['resources']['disk']

        return self._db.get_flavour(self._identity, cloud, required_cores, required_memory, required_disk)

    def get_image(self, cloud):
        """
        Return the image matching the job
        """
        image_name = None
        required_image_distribution = None
        required_image_version = None
        required_image_type = None
        required_image_architecture = None

        if 'requirements' not in self._job:
            return image_name

        if 'image' in self._job['requirements']:
            if 'distribution' in self._job['requirements']['image']:
                required_image_distribution = self._job['requirements']['image']['distribution']

            if 'version' in self._job['requirements']['image']:
                required_image_version = self._job['requirements']['image']['version']

            if 'type' in self._job['requirements']['image']:
                required_image_type = self._job['requirements']['image']['type']

            if 'architecture' in self._job['requirements']['image']:
                required_image_architecture = self._job['requirements']['image']['architecture']

        return self._db.get_image(self._identity,
                                  cloud,
                                  required_image_type,
                                  required_image_architecture,
                                  required_image_distribution,
                                  required_image_version)

    def satisfies_sites(self):
        """
        Returns list of clouds satisfying site requirements
        """
        clouds_out = self._clouds.copy()

        if 'requirements' not in self._job:
            return clouds_out

        if 'sites' in self._job['requirements']:
            for cloud in self._clouds:
                if cloud not in self._job['requirements']['sites']:
                    clouds_out.remove(cloud)

        return clouds_out

    def satisfies_regions(self):
        """
        Returns list of clouds satisfying region requirements
        """
        clouds_out = self._clouds.copy()

        if 'requirements' not in self._job:
            return clouds_out

        if 'regions' not in self._job['requirements']:
            return clouds_out

        if not self._job['requirements']['regions']:
            return clouds_out

        if 'regions' in self._job['requirements']:
            for cloud in self._clouds:
                if self._config[cloud]['region'] not in self._job['requirements']['regions']:
                    clouds_out.remove(cloud)

        return clouds_out

    def satisfies_flavour(self):
        """
        Returns list of clouds satisfying flavour requirements
        """
        clouds_out = self._clouds.copy()

        for cloud in self._clouds:
            if not self.get_flavour(cloud):
                clouds_out.remove(cloud)

        return clouds_out

    def satisfies_image(self):
        """
        Returns list of clouds satisfying image requirements
        """
        clouds_out = self._clouds.copy()

        for cloud in self._clouds:
            if not self.get_image(cloud):
                clouds_out.remove(cloud)

        return clouds_out

    def satisfies_group(self):
        """
        Returns list of clouds satisfying group requirements
        """
        clouds_out = self._clouds.copy()

        for cloud in self._clouds:
            found = False
            if 'supported_groups' in self._config[cloud]:
                if not self._config[cloud]['supported_groups']:
                    found = True

                for group in self._config[cloud]['supported_groups']:
                    if 'requirements' in self._job:
                        if 'groups' in self._job['requirements']:
                            for mygroup in self._job['requirements']['groups']:
                                if mygroup == group:
                                    found = True

            else:
                found = True

            if not found:
                clouds_out.remove(cloud)

        return clouds_out

    def satisfies_dynamic_quotas(self):
        """
        Returns list of clouds with enough resources available currently to run the job
        """
        clouds_out = self._clouds.copy()

        for cloud in self._clouds:
            (_, _, _, _, _, remaining_cpus, remaining_memory, remaining_instances) = self._db.get_cloud_info(cloud, self._identity)
            instances = self._job['requirements']['resources']['instances']

            if remaining_instances != -1 and self._job['requirements']['resources']['instances'] > remaining_instances:
                clouds_out.remove(cloud)

            if remaining_cpus != -1 and self._job['requirements']['resources']['cores']*instances > remaining_cpus:
                clouds_out.remove(cloud)

            if remaining_memory != -1 and self._job['requirements']['resources']['memory']*instances > remaining_memory:
                clouds_out.remove(cloud)

        return clouds_out

    def satisfies_status(self):
        """
        Returns list of clouds with up status or no status
        """
        clouds_out = self._clouds.copy()

        for cloud in self._clouds:
            (status, mon_status, _, _, _, _, _, _) = self._db.get_cloud_info(cloud, self._identity)
            if status == 1 or mon_status == 1:
                clouds_out.remove(cloud)

        return clouds_out

    def satisfies_static_quotas(self):
        """
        Returns list of clouds with enough resources in the static quotas
        """
        clouds_out = self._clouds.copy()

        for cloud in self._clouds:
            (_, _, limit_cpus, limit_memory, limit_instances, _, _, _) = self._db.get_cloud_info(cloud, self._identity)
            instances = self._job['requirements']['resources']['instances']

            if limit_cpus != -1 and self._job['requirements']['resources']['cores']*instances > limit_cpus:
                clouds_out.remove(cloud)

            if limit_memory != -1 and self._job['requirements']['resources']['memory']*instances > limit_memory:
                clouds_out.remove(cloud)

            if limit_instances != -1 and instances > limit_instances:
                clouds_out.remove(cloud)

        return clouds_out

    def statisfies_requirements(self, ignore_usage=False):
        """
        Returns list of clouds meeting all requirements
        """
        clouds = [self.satisfies_sites(),
                  self.satisfies_regions(),
                  self.satisfies_image(),
                  self.satisfies_flavour(),
                  self.satisfies_group(),
                  self.satisfies_static_quotas(),
                  self.satisfies_status()]

        if not ignore_usage:
            clouds.append(self.satisfies_dynamic_quotas())

        # Create a list of clouds matching all requirements
        clouds = list(set(clouds[0]).intersection(*clouds))

        # Print information in the log file to help understanding decisions
        logger.info('Clouds matching sites: %s', ','.join(self.satisfies_sites()))
        logger.info('Clouds matching regions: %s', ','.join(self.satisfies_regions()))
        logger.info('Clouds matching image: %s', ','.join(self.satisfies_image()))
        logger.info('Clouds matching flavour: %s', ','.join(self.satisfies_flavour()))
        logger.info('Clouds matching group: %s', ','.join(self.satisfies_group()))
        logger.info('Clouds matching static quotas: %s', ','.join(self.satisfies_static_quotas()))
        logger.info('Clouds matching status: %s', ','.join(self.satisfies_status()))
        if not ignore_usage:
            logger.info('Clouds matching dynamic quotas: %s', ','.join(self.satisfies_dynamic_quotas()))

        return clouds

    def rank(self, clouds):
        """
        Returns ranked list of clouds
        """
        if 'preferences' not in self._job:
            return clouds

        ranking_sites = {}
        ranking_regions = {}

        for cloud in clouds:
            ranking_sites[cloud] = 0
            ranking_regions[cloud] = 0

        if 'regions' in self._job['preferences']:
            for count in range(0, len(self._job['preferences']['regions'])):
                for cloud in clouds:
                    if self._config[cloud]['region'] == self._job['preferences']['regions'][count]:
                        ranking_regions[cloud] = len(self._job['preferences']['regions']) - count

        if 'sites' in self._job['preferences']:
            for count in range(0, len(self._job['preferences']['sites'])):
                for cloud in clouds:
                    if cloud == self._job['preferences']['sites'][count]:
                        ranking_sites[cloud] = len(self._job['preferences']['sites']) - count

        # Get list of clouds with numbers of successful and failed deployments
        failures = self._db.get_deployment_failures(self._identity, 2*60*60)
        successes = self._db.get_deployment_failures(self._identity, 2*60*60, True)

        ratios = {}
        for cloud in set(successes).intersection(failures):
            ratios[cloud] = successes[cloud]/(successes[cloud] + failures[cloud])
            logger.info('Cloud %s has success ratio of %d', cloud, 100*ratios[cloud])

        for cloud in successes:
            if cloud not in ratios:
                ratios[cloud] = 1.0

        for cloud in failures:
            if cloud not in ratios:
                ratios[cloud] = 0.0

        cloud_weights = {}
        for cloud in clouds:
            # TODO: this is not really correct!
            # TODO: failed state can be overriden if dynamic quotas change
            weight_failures = 0
            if cloud in ratios:
                weight_failures = 1000*ratios[cloud]

            weight = ranking_regions[cloud] + ranking_sites[cloud] + weight_failures
            cloud_weights[cloud] = weight

        return sorted(cloud_weights, key=cloud_weights.__getitem__, reverse=True)
