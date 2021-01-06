import rpyc
from rpyc.utils.server import ThreadedServer
import hashlib
import time
from wordcount_mapper import mapper_wc
from invertedindex_mapper import mapper_ii


def hash_(string):
    return int(hashlib.sha512(string.encode('utf-8')).hexdigest(), 16)%2

class MapperService(rpyc.Service):
    def __init__(self):
        pass

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass
    
    def exposed_execute(self, i, task, filename, kv_server_address, kvport, num_reducers):
        filename = "mapper_input"+str(i)+".txt"
        kv = rpyc.connect(kv_server_address, kvport, config={'allow_pickle':True, 'allow_public_attrs':True}).root
        # ms = rpyc.connect("localhost", master_port, config={'allow_pickle':True, 'allow_public_attrs':True}).root
        print(f"mapper {i} connected")
        # logging.info('mapper %s connected',str(i))
        try:
            if task == 'word_count':
                data = eval(kv.get(filename))
                map_output = mapper_wc(data)
                map_split = [[] for i in range(num_reducers)]
                for pair in map_output:
                    map_split[hash_(pair[0])%num_reducers].append(pair)
                kv.map_set(map_split)
            elif task == 'inverted_index':
                file_map = eval(kv.get("file_mapping.txt"))
                file = file_map[filename]
                file = '/'.join(file.split('\\'))
                data = kv.get(file)
                map_output = mapper_ii(data,file_map[filename].split('\\')[-1].split('.')[0])
                map_split = [[] for i in range(num_reducers)]
                for pair in map_output:
                    map_split[hash_(pair[0])%num_reducers].append(pair)
                kv.map_set(map_split)
            else:
                print('Not a valid application')
                # logging.error('Not a valid application')
            # ms.map_ack(i)
        except Exception:
            print(f"Exception occurred in mapper {i}")
        #     logging.error(f"Exception occurred in mapper {i}", exc_info=True)
        # logging.info('mapper %s task successful',str(i))
            
def main(port = 3389):
    time.sleep(10)
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(MapperService, port = port, 
                       protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
    try:
        print('worker started on port: ', port)
        t.start()
    except Exception:
        t.stop()

if __name__ == '__main__':
    main()