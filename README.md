# IMC

## Overview

It is frequently assumed that when you're using a cloud you have access to an essentially infinite amount of resources, however this is not always the case. IMC is for situations when you have access to many small clouds and you need a tool which can deploy and configure virtual infrastructure across them.

Features include:
* selection of clouds which meet specified requirements
  * e.g. I want to deploy a CentOS 7 VM with at least 8 cores and 32 GB of RAM
* clouds are ranked based on specified preferences
  * e.g. I would prefer my VMs to be deployed on my local private cloud, but if that is full try my national research cloud, but if that is also full then use a public cloud
* if deployment on a cloud fails, another cloud will be automatically tried
* many types of failures and issues are handled automatically, including:
  * deployment failing completely
  * contextualization failure
  * infrastructure taking too long to deploy or configure
* clouds can be grouped into regions

IMC uses [Infrastructure Manager](https://github.com/grycap/im) to deploy and configure infrastructure on clouds, including OpenStack, AWS, Azure and Google Compute Platform. It can use either Ansible or Cloud-Init for contextualization. [Open Policy Agent](https://www.openpolicyagent.org) is used for making decisions about what clouds, VM flavours and images to use.

## Configuration
A JSON document in the following form is used to provide static information about known clouds to OPA:
```json
{
   "clouds":{
       "cloud1":{...},
       "cloud2":{...},
       ...
       "cloudn":{...}    
   }
}
```
Configuration for a single cloud has the form:
```json
{
   "name":"<name>",
   "region":"<region>",
   "quotas":{
       "cores":i,
       "instances":j
   },
   "images":{
       "<id>":{
           "name":"<name>",
           "architecture":"<arch>",
           "distribution":"<dist>",
           "type":"<type>",
           "version":"<version>"
        }   
   },
   "flavours":{
       "<id>":{
           "name":"<name>",
           "cores":i,
           "memory":j,
           "tags":{
           },
       } 
   }
}
```
The image name should be in a form directly useable by IM, for example `gce://europe-west2-c/centos-7` (for Google) or `ost://<openstack-endpoint>/<id>` (for OpenStack). Meta-data is provided for each image to easily enable users to select a standard Linux distribution image at any site, e.g. CentOS 7 or Ubuntu 16.04, without needing to know in advance the image name at each site.

Each flavour has an optional `tags`, which should contain key-value pairs. This can be used to specify additional information about the VM flavour, for example:
```json
"tags":{
    "infiniband":"true"
}
```
Tags can be taken into account with requirements and preferences.
