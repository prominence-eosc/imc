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
* can automatically deploy Ansible hosts with public IP addresses, with only one per cloud, in order to use Ansible for contextualization

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

## Deployment
Deploy Infrastructure Manager following the instructions https://github.com/grycap/im. Alternatively an existing deployment can be used. For testing it is adequate to run the IM Docker container:
```
docker run -d --name=im -p 127.0.0.1:8899:8899 grycap/im:1.7.4
```

Deploy Open Policy Agent:
```
docker run -p 127.0.0.1:8181:8181 -v <directory>:/policies --name=opa -d openpolicyagent/opa:latest run --server /policies
```
where `<directory>` should be replaced with the path to the directory on the host containing the policy and data files (i.e. the contents of https://github.com/alahiff/imc/tree/master/policies).

Deploy the IM client. The simplest way to do this is using pip:
```
pip install IM-client==1.5.1
```
It is important to note that 1.5.2 and 1.5.3 cannot be used as they return incorrect exit codes.

In the home directory of the user which will run IMC, create a file `.im_client.cfg`:
```
[im_client]
xmlrpc_url=http://localhost:8899
auth_file=/home/cloudadm/.im_auth.dat
```
This should be adjusted as necessary to point to the IM XML-RPC service and to the IM client authorization file, which should list all required clouds. See http://www.grycap.upv.es/im/documentation.php for information on what should appear in this file.

## RADL files

IM uses Resource and Application Description Language (RADL) files to describe the infrastructure to be deployed. IMC must be provided with a RADL file, noting that:
* `${image}` will be replaced with the disk image name (essential)
* `${instance}` will be replaced with the instance type (essential)
* `${cloud}` will be replaced with the name of the cloud
* `${ansible_ip}` will be replaced with the public IP address of an appropriate Ansible machine (if needed)
* `${ansible_username}` will be replaced with the username of an appropriate Ansible machine (if needed)
* `${ansible_private_key}` will be replaced with the private key of an appropriate Ansible machine (if needed)

## Usage

