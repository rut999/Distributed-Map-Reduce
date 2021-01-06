import rpyc
from rpyc.utils.server import ThreadedServer
import hashlib
import time
from wordcount_reducer import reducer_wc
from invertedindex_reducer import reducer_ii



class ReducerService(rpyc.Service):
    def __init__(self):
        pass

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass
    
    def exposed_execute(self, i, task, filename, kv_server_address, kvport):
        kv = rpyc.connect(kv_server_address, kvport, config={'allow_pickle':True, 'allow_public_attrs':True}).root
        print(f"reducer {i} connected")
        # logging.info('reducer %s connected',str(i))
        filename = "reducer_input"+str(i)+".txt"
        data = kv.get(filename)
        pairs = data.split()
        try:
            if task == 'word_count':
                red_output = reducer_wc(pairs)
                kv.red_set(red_output)
            elif task == 'inverted_index':
                file_map = eval(kv.get("file_mapping.txt"))
                files = [val.split('\\')[-1].split('.')[0] for val in file_map.values()]
                red_output = reducer_ii(pairs,files)
                kv.red_set(red_output)
            else:
                print('Not a valid application')
                # logging.error('Not a valid application')
        except Exception:
            print(f"Exception occurred in reducer {i}")
            # logging.error(f"Exception occurred in reducer {i}", exc_info=True)
        # logging.info('reducer %s task successful',str(i))
            
def main(port = 3389):
    time.sleep(10)
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(ReducerService, port = port, 
                       protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
    try:
        print('worker started on port: ', port)
        t.start()
    except Exception:
        t.stop()

if __name__ == '__main__':
    main()