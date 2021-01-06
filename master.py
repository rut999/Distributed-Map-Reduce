
import gcp_instance
import rpyc
import os, time
from rpyc.utils.server import ThreadedServer
from multiprocessing import Process, Queue
import glob
from os.path import join
import configparser
import logging

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
log_file="master_log.log"

logging.basicConfig(filename=log_file,
                    format="[%(levelname)s] Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s",
                    level=logging.INFO)

class Master(rpyc.Service):
    def __init__(self):
        self.gcpapi = gcp_instance.GCP_API()
        
        #config parameters
        self.config = configparser.ConfigParser()
        self.config.read('config.ini') 
        self.kvport = int(self.config['KVServer']['kvport'])
        self.master_port = int(self.config['Master']['master_port'])
        self.project = self.config['GCP']['project_id']
        self.zone = self.config['GCP']['zone']
        
        self.mapper_ext_ips = []
        self.reducer_ext_ips = []

    def on_connect(self, conn):
        self.mapper_connections = []
        self.reducer_connections = []
        pass

    def on_disconnect(self, conn):
        pass
        
    def create_node(self, ind, queue,worker):
        logging.info(f'Attempting to create {worker+str(ind)}')
        self.gcpapi.create_instance(self.project, self.zone, worker+str(ind))
        _, ext_ip = self.gcpapi.getIPAddresses(self.project, self.zone, worker+str(ind))
        logging.info(f"{worker+str(ind)} instance created")
        queue.put(ext_ip)
                
    def spawn_worker(self, nworkers, worker_ext_ips, port, worker_connections,worker_type):
        for ind in range(nworkers):
            logging.info(f'Attempting to create {worker_type+str(ind)}')
            self.gcpapi.create_instance(self.project, self.zone, worker_type+str(ind))
            _, ext_ip = self.gcpapi.getIPAddresses(self.project, self.zone, worker_type+str(ind))
            logging.info(f"{worker_type+str(ind)} instance created")
            worker_ext_ips.append(ext_ip)
            
        # Wait until all mapper vms are created
        while len(worker_ext_ips) != nworkers:
            continue
        
        # connecting to all workers
        for ip in worker_ext_ips:
            port = int(port)
            worker_started = False
            
            logging.info(f'Trying to connect instance on ip: {ip}')
            while not worker_started:
                # Wait till starting rpyc server on worker node...
                try:
                    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
                    conn = rpyc.connect(str(ip), port=port, config = rpyc.core.protocol.DEFAULT_CONFIG)
                    worker_started = True
                    worker_connections.append(conn.root)
                    print('CONNECTED')
                except Exception:
                    time.sleep(1)

               
        
    def exposed_init_cluster(self, n_mappers, n_reducers):
        try:
            _, self.KVServer_address = self.gcpapi.getIPAddresses(self.project, self.zone, self.config['KVServer']['kvserver_name'])
            logging.info('KVServer instance already exists..')
        except:
            logging.info('Starting KVServer...')
            self.gcpapi.create_instance(self.project, self.zone, self.config['KVServer']['kvserver_name'])
            _, self.KVServer_address = self.gcpapi.getIPAddresses(self.project, self.zone, self.config['KVServer']['kvserver_name'])

        while True:
            try:
                rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
                self.KVServer = rpyc.connect(self.KVServer_address, self.kvport, config={'allow_pickle':True, 'allow_public_attrs':True}).root
                logging.info('Master CONNECTED to KV Server')
                break
            except:
                continue

        self.nmapper = n_mappers
        self.nreducer= n_reducers
        self.spawn_worker(self.nmapper, self.mapper_ext_ips,self.config['Worker']['worker_port'], self.mapper_connections,"mapper")
        self.spawn_worker(self.nreducer, self.reducer_ext_ips,self.config['Worker']['worker_port'], self.reducer_connections,"reducer")
        return int(time.time())
    
    def input_chunks(self):
        
        try:
            if self.task == 'word_count':
                data_chunks = []
                f = open(self.filename, "r")
                size = os.stat(self.filename).st_size
                for i in range(self.nmapper):
                    data_chunk = f.readlines(size//self.nmapper)
                    data_chunks.append(data_chunk)
                self.KVServer.set_data_chunk(data_chunks,self.nreducer,self.task)
                  
            elif self.task == 'inverted_index':
                file_mapping = {}
                files = glob.glob(join(self.filename,"*.txt"))
                if len(files) == self.nmapper:
                    for i in range(self.nmapper):
                        file_mapping["mapper_input"+str(i)+".txt"] = files[i]
                    self.KVServer.set_data_chunk(file_mapping,self.nreducer,self.task)
                else:
                    logging.info('Number of mappers should be equal to number of text documents')
            else:
                logging.error('Not a valid application')
        except Exception:
            logging.error("Exception occurred")
        logging.info(f"Input data chunks created")
        
    def exposed_run_mapreduce(self, data_file, function):
        self.task = function
        self.filename = data_file
        
        #Divide input data into chunks
        self.input_chunks()
        
        mappers = []
        for i, mapper in enumerate(self.mapper_connections):
            mappers.append(rpyc.async_(mapper.execute)(i, self.task, self.filename, self.KVServer_address, 
                                                       self.kvport, self.nreducer))
            mappers[i].set_expiry(None)

        # wait till all mappers to completes their assigned task
        for mapper in mappers:
            while not mapper.ready:
                continue
        # logging.info('Mappers have completed their assigned task...')
        
        reducers = []
        
        for i, reducer in enumerate(self.reducer_connections):
            reducers.append(rpyc.async_(reducer.execute)(i, self.task, self.filename, self.KVServer_address, 
                                                       self.kvport))
            reducers[i].set_expiry(None)

        # wait till all reducers to completes their assigned task
        for reducer in reducers:
            while not reducer.ready:
                continue
     
    def exposed_destroy_cluster(self):
        
        # Terminate and delete all worker instances
        for ind in range(self.nmapper):
            self.gcpapi.delete_instance(self.project, self.zone, "mapper"+str(ind))
        for ind in range(self.nreducer):
            self.gcpapi.delete_instance(self.project, self.zone, "reducer"+str(ind))

        # Terminate KVServer
        logging.info('Cluster destroyed')
        
    
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    port = int(config['Master']['master_port'])
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(Master, port = port, protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
    try:
        t.start()
    except Exception:
        t.stop()   