
import os, time
import googleapiclient.discovery
from google.oauth2 import service_account
import configparser

class GCP_API():
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.scopes = ['https://www.googleapis.com/auth/cloud-platform']
        self.sa_file = self.config['GCP']['credentials_file']
        self.image = self.config['GCP']['image']
        credentials = service_account.Credentials.from_service_account_file(self.sa_file, scopes=self.scopes)
        self.compute = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
        
    # [START create_instance]
    def create_instance(self, project, zone, name):
        
        
        image_response = self.compute.images().get(
            project=project, image=self.image).execute()
        
        # Get the latest Debian Jessie image.
        # image_response = compute.images().getFromFamily(
        #     project='debian-cloud', family='debian-9').execute()
        source_disk_image = image_response['selfLink']
    
        # Configure the machine
        machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
        if name == "kvserver":
            startup_script = open(
                os.path.join(
                    os.path.dirname(__file__), 'kv_startup_script.sh'), 'r').read()
        elif name == "master":
            startup_script = open(
                os.path.join(
                    os.path.dirname(__file__), 'master_startup_script.sh'), 'r').read()
        elif name[:-1] == "mapper":
            startup_script = open(
                os.path.join(
                    os.path.dirname(__file__), 'mapper_startup_script.sh'), 'r').read()
        else:
            startup_script = open(
                os.path.join(
                    os.path.dirname(__file__), 'reducer_startup_script.sh'), 'r').read()
        image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
        image_caption = "Ready for dessert?"
    
        config = {
            'name': name,
            'machineType': machine_type,
    
            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
                    }
                }
            ],
    
            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],
    
            # Allow the instance to access cloud storage and logging.
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],
    
            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }, {
                    'key': 'url',
                    'value': image_url
                }, {
                    'key': 'text',
                    'value': image_caption
                }]
            }
        }
        
        # instance = self.compute.instances().insert(project=project,zone=zone,body=config).execute()
        # self.wait_for_operation(project, zone, instance['name'])
        stat = 'NOT DONE'
        while stat != 'DONE':
            try:
                instance = self.compute.instances().insert(project=project,zone=zone,body=config).execute()
                stat = self.wait_for_operation(project, zone, instance['name'])
                break
            except:
                continue
    # [END create_instance]
    
    # [START wait_for_operation]
    def wait_for_operation(self, project, zone, operation):
        print('Waiting for operation to finish...')
        while True:
            result = self.compute.zoneOperations().get(
                project=project,
                zone=zone,
                operation=operation).execute()
    
            if result['status'] == 'DONE':
                print("done.")
                if 'error' in result:
                    raise Exception(result['error'])
                return result
    
            time.sleep(1)
    # [END wait_for_operation]
            
    # [START delete_instance]
    def delete_instance(self, project, zone, name):
        return self.compute.instances().delete(
            project=project,
            zone=zone,
            instance=name).execute()
    # [END delete_instance]
    
    # [START getIPAddresses]
    def getIPAddresses(self, project, zone, name):
        instance = self.compute.instances().get(project=project, zone=zone, instance=name).execute()
        external_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        internal_ip = instance['networkInterfaces'][0]['networkIP']
        return internal_ip, external_ip
    # [END getIPAddresses]