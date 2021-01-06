import gcp_instance
import rpyc
import configparser
import time

#config parameters
config = configparser.ConfigParser()
config.read('config.ini') 
kvserver_name = config['KVServer']['kvserver_name']
master_name = config['Master']['master_name']
masterport = config['Master']['master_port']
project = config['GCP']['project_id']
zone = config['GCP']['zone']


start_time = time.time()
# #create Master Server instance
ms_inst = gcp_instance.GCP_API()
ms_inst.create_instance(project, zone, master_name)
_, masterAddress = ms_inst.getIPAddresses(project, zone, master_name)
print("Master Created")

#create KVServer instance
kv_inst = gcp_instance.GCP_API()
print(kv_inst.create_instance(project, zone, kvserver_name))
print("KV Server Created")

#connect with master
master_started = False
            
print(f'Trying to connect instance on ip: {masterAddress} and port: {masterport}')
while not master_started:
    # Wait till starting rpyc server on master node...
    try:
        rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
        master_conn = rpyc.connect(masterAddress, masterport, config={'allow_pickle':True, 'allow_public_attrs':True}).root
        master_started = True
        print('CONNECTED')
    except Exception:
        time.sleep(1)


# USER DEFINED VARIABLES
task = config['UserInputs']['applicaton'] #'inverted_index'
num_mappers = int(config['UserInputs']['num_mappers'])
num_reducers = int(config['UserInputs']['num_reducers'])
input_data_file = config['UserInputs']['document_or_directory']

# Calling main APIs
# init cluster
print('Starting init cluster...')
master_conn.init_cluster(num_mappers, num_reducers)
print('Cluster created')
print(f"Time taken till creating cluster is {time.time()-start_time}")

# run map reduce
print('Starting run map reduce...')
master_conn.run_mapreduce(input_data_file, task)
print('Completed running map reduce')

# destroy cluster
print('Starting to destroy cluster...')
master_conn.destroy_cluster()
### uncomment below lines to delete master and keyvalue VM instances 
# ms_inst.delete_instance(project, zone, master_name)
# kv_inst.delete_instance(project, zone, kvserver_name)
print('Cluster destroyed')

end_time = time.time()-start_time
print(f"Time taken for the whole process is {end_time}")

                