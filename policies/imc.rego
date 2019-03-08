package imc

import data.clouds
import data.status

# Regions
satisfies_region(cloud) {
  not input.requirements.regions
}

satisfies_region(cloud) {
  input.requirements.regions[i] = cloud.region
}

# Sites
satisfies_site(cloud) {
  not input.requirements.sites
}

satisfies_site(cloud) {
  input.requirements.sites[i] = cloud.name
}

# Tags
satisfies_tags(cloud) {
  not input.requirements.tags
}

satisfies_tags(cloud) {
  input.requirements.tags[i] = cloud.tags[i]
}

# Images
satisfies_image_architecture(image){
  input.requirements.image.architecture = image.architecture
}

satisfies_image_architecture(image){
  not input.requirements.image.architecture
}

satisfies_image_distribution(image){
  input.requirements.image.distribution = image.distribution
}

satisfies_image_distribution(image){
  not input.requirements.image.distribution
}

satisfies_image_type(image){
  input.requirements.image.type = image.type
}

satisfies_image_type(image){
  not input.requirements.image.type
}

satisfies_image_version(image){
  input.requirements.image.version = image.version
}

satisfies_image_version(image){
  not input.requirements.image.version
}

satisfies_image_name(image){
  input.requirements.image.name = image.name
}

satisfies_image_name(image){
  not input.requirements.image.name
}

satisfies_image(image) {
  not input.requirements.image
}

satisfies_image(image) {
  satisfies_image_architecture(image)
  satisfies_image_distribution(image)
  satisfies_image_type(image)
  satisfies_image_version(image)
  satisfies_image_name(image)
}

# Flavours
satisfies_flavour(flavour) {
  satisfies_flavour_resources(flavour)
  satisfies_flavour_tags(flavour)
}

satisfies_flavour_resources(flavour) {
  flavour.cores >= input.requirements.resources.cores
  flavour.memory >= input.requirements.resources.memory
}

satisfies_flavour_resources(flavour) {
  not input.requirements.resources.cores
  not input.requirements.resources.memory
}

satisfies_flavour_tags(flavour) {
  not input.requirements.resources.tags
}

satisfies_flavour_tags(flavour) {
  input.requirements.resources.tags[i] = flavour.tags[i]
}

# Quotas - static
satisfies_static_quotas(cloud) {
  input.requirements.resources.cores * input.requirements.resources.instances <= cloud.quotas.cores
  input.requirements.resources.instances <= cloud.quotas.instances
}

satisfies_static_quotas(cloud) {
  input.requirements.resources.cores * input.requirements.resources.instances <= cloud.quotas.cores
  not cloud.quotas.instances
}

satisfies_static_quotas(cloud) {
  not cloud.quotas
}

satisfies_dynamic_quotas(cloud) {
  input.requirements.resources.cores * input.requirements.resources.instances <= status[cloud.name].quota.cpus
  input.requirements.resources.memory * input.requirements.resources.instances <= status[cloud.name].quota.memory
  input.requirements.resources.instances <= status[cloud.name].quota.instances
}

# Quotas - dynamic
satisfies_dynamic_quotas(cloud) {
  not status[cloud.name].quota
}

# Get list of sites meeting requirements
sites[site] {
  cloud = clouds[site]
  image = clouds[site]["images"][i]
  flavour = clouds[site]["flavours"][j]
  satisfies_region(cloud)
  satisfies_site(cloud)
  satisfies_tags(cloud)
  satisfies_image(image)
  satisfies_flavour(flavour)
  satisfies_dynamic_quotas(cloud)
  satisfies_static_quotas(cloud)
}

# Get images for a specified cloud
images[name] {
  image = clouds[input.cloud]["images"][i]
  name = image.name
  satisfies_image(image)
}

# Rank flavours for a specified cloud
flavours[pair] {
  flavour =  clouds[input.cloud]["flavours"][i]
  satisfies_flavour(flavour)
  weight = flavour_weight(flavour)
  pair = {"name":flavour.name, "weight":weight}
}

# Rank sites based on preferences
rankedsites[pair] {
  weight = region_weight(site)
  site = input.clouds[i]
  pair = {"site":site, "weight":weight}
}

# Region weight
region_weight(site) = output {
  cloud = clouds[site]
  i = cloud.region
  output = input.preferences.regions[i]
}

region_weight(site) = output {
  cloud = clouds[site]
  i = cloud.region
  not input.preferences.regions[i]
  output = 0
}

# Flavour weight - resources
flavour_weight(flavour) = output {
  output = flavour.cost
}

flavour_weight(flavour) = output {
  not flavour.cost
  output = flavour.cores * flavour.memory
}

# Flavour weight - tags
flavour_tags_weight(flavour) = output {
  not input.preferences.tags
  output = 0
}

flavour_tags_weight(flavour) = output {
  not flavour.tags
  output = 0
}

flavour_tags_weight(flavour) = output {
  input.preferences.tags[i] = flavour.tags[i]
  output = 1
}
