import sys
import traceback
import socket
import os
from argparse import ArgumentParser
import requests
import time
import datetime
import random
import threading
import multiprocessing
import json 
import yaml
from dxf import *
from multiprocessing import Process, Queue
import importlib
import hash_ring
from collections import defaultdict

input_dir = '/home/nannan/dockerimages/docker-traces/data_centers/'

## get requests
def send_request_get(client, payload):
    ## Read from the queue
    s = requests.session()
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    s.post("http://" + str(client) + "/up", data=json.dumps(payload), headers=headers, timeout=100)

def send_warmup_thread(requests, q, registry, generate_random):
    trace = {}
    dxf = DXF(registry, 'test_repo', insecure=True)
#     f = open(str(os.getpid()), 'wb')
#     f.write('\0')
#     f.close()
    for request in requests:
        if request['size'] < 0:
            trace[request['uri']] = 'bad'
#         elif not (request['uri'] in trace):
#             with open(str(os.getpid()), 'wb') as f:
#                 if generate_random is True:
#                     f.seek(request['size'] - 9)
#                     f.write(str(random.getrandbits(64)))
#                     f.write('\0')
#                 else:
#                     f.seek(request['size'] - 1)
#                     f.write('\0')

        try:
            dgst = dxf.push_blob(request['data'])
        except:
            dgst = 'bad'
        print request['uri'], dgst
        trace[request['uri']] = dgst
#     os.remove(str(os.getpid()))
    q.put(trace)

def warmup(data, out_trace, registry, threads, generate_random):
    trace = {}
    processes = []
    q = Queue()
    process_data = []
    for i in range(threads):
        process_data.append([])
    i = 0
    for request in data:
        if request['method'] == 'GET':
            process_data[i % threads].append(request)
            i += 1
    for i in range(threads):
        p = Process(target=send_warmup_thread, args=(process_data[i], q, registry, generate_random))
        processes.append(p)

    for p in processes:
        p.start()

    for i in range(threads):
        d = q.get()
        for thing in d:
            if thing in trace:
                if trace[thing] == 'bad' and d[thing] != 'bad':
                    trace[thing] = d[thing]
            else:
                trace[thing] = d[thing]

    for p in processes:
        p.join()

    with open(out_trace, 'w') as f:
        json.dump(trace, f)
 
def stats(responses):
    responses.sort(key = lambda x: x['time'])

    endtime = 0
    data = 0
    latency = 0
    total = len(responses)
    onTimes = 0
    failed = 0
    wrongdigest = 0
    startTime = responses[0]['time']
    for r in responses:
#         if r['onTime'] == 'failed':
        if "failed" in r['onTime']:
            total -= 1
            failed += 1
            continue
        if r['time'] + r['duration'] > endtime:
            endtime = r['time'] + r['duration']
        latency += r['duration']
        data += r['size']
        if r['onTime'] == 'yes':
            onTimes += 1
        if r['onTime'] == 'yes: wrong digest':
            wrongdigest += 1
            
    duration = endtime - startTime
    print 'Statistics'
    print 'Successful Requests: ' + str(total)
    print 'Failed Requests: ' + str(failed)
    print 'Wrong digest requests: '+str(wrongdigest)
    print 'Duration: ' + str(duration)
    print 'Data Transfered: ' + str(data) + ' bytes'
    print 'Average Latency: ' + str(latency / total)
    print '% requests on time: ' + str(1.*onTimes / total)
    print 'Throughput: ' + str(1.*total / duration) + ' requests/second'

           
def serve(port, ids, q, out_file):
    server_address = ("0.0.0.0", port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(server_address)
        sock.listen(len(ids))
    except:
        print "Port already in use: " + str(port)
        q.put('fail')
        quit()
    q.put('success')
 
    i = 0
    response = []
    print "server waiting"
    while i < len(ids):
        connection, client_address = sock.accept()
        resp = ''
        while True:
            r = connection.recv(1024)
            if not r:
                break
            resp += r
        connection.close()
        try:
            info = json.loads(resp)
            if info[0]['id'] in ids:
                info = info[1:]
                response.extend(info)
                i += 1
        except:
            print 'exception occured in server'
            pass

    with open(out_file, 'w') as f:
        json.dump(response, f)
    print 'results written to ' + out_file
    stats(response)

  
## Get blobs
def get_blobs(data, clients_list, port, out_file):
    processess = []

    ids = []
    for d in data:
        ids.append(d[0]['id'])

    serveq = Queue()
    server = Process(target=serve, args=(port, ids, serveq, out_file))
    server.start()
    status = serveq.get()
    if status == 'fail':
        quit()
    ## Lets start processes
    i = 0
    for client in clients_list:
        p1 = Process(target = send_request_get, args=(client, data[i], ))
        processess.append(p1)
        i += 1
        print "starting client ..."
    for p in processess:
        p.start()
    for p in processess:
        p.join()

    server.join()

######
# NANNAN: trace_file+'-realblob.json'
######
# def get_requests(files, t, limit):
#     ret = []
#     for filename in files:
#         with open(filename+'-realblob.json', 'r') as f:
#             requests = json.load(f)
#     
#         for request in requests:
#             method = request['http.request.method']
#             uri = request['http.request.uri']
#             if (('GET' == method) or ('PUT' == method)) and (('manifest' in uri) or ('blobs' in uri)):
#                 size = request['http.response.written']
#                 if size > 0:
#                     timestamp = datetime.datetime.strptime(request['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
#                     duration = request['http.request.duration']
#                     client = request['http.request.remoteaddr']
#                     blob = request['data']
#                     r = {
#                         'delay': timestamp, 
#                         'uri': uri, 
#                         'size': size, 
#                         'method': method, 
#                         'duration': duration,
#                         'client': client,
#                         'data': blob
#                     }
#                     ret.append(r)
#     ret.sort(key= lambda x: x['delay'])
#     begin = ret[0]['delay']
# 
#     for r in ret:
#         r['delay'] = (r['delay'] - begin).total_seconds()
#    
#     if t == 'seconds':
#         begin = ret[0]['delay']
#         i = 0
#         for r in ret:
#             if r['delay'] > limit:
#                 break
#             i += 1
#         print i 
#         return ret[:i]
#     elif t == 'requests':
#         return ret[:limit]
#     else:
#         return ret


def absoluteFilePaths(directory):
    absFNames = []
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            absFNames.append(os.path.abspath(os.path.join(dirpath, f)))
            
    return absFNames

####
# Random match
####

def get_requests(trace_dir):
    print "walking trace_dir: "+trace_dir
    absFNames = absoluteFilePaths(trace_dir)

    blob_locations = []
#     tTOblobdic = {}
#     blobTOtdic = {}
    ret = []
#     i = 0
    
#     for location in realblob_locations:
#         absFNames = absoluteFilePaths(location)
    print "Dir: "+trace_dir+" has the following files"
    print absFNames
    blob_locations.extend(absFNames)
    
    for trace_file in blob_locations:
        with open(trace_file, 'r') as f:
            requests = json.load(f)
            
        for request in requests:
            
            method = request['http.request.method']
            uri = request['http.request.uri']
    	    if len(uri.split('/')) < 3:
                continue
            # ignore othrs reqs, because using push and pull reqs.
            if (('GET' == method) or ('PUT' == method)) and (('manifest' in uri) or ('blobs' in uri)):
                size = request['http.response.written']
                if size > 0:
#                     timestamp = datetime.datetime.strptime(request['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
#                     duration = request['http.request.duration']
#                     client = request['http.request.remoteaddr']
            
#                     for blob in blob_locations:
#                     if i < len(blob_locations):
#                         blob = blob_locations[i]
                        #uri = request['http.request.uri']
#                         if layer_id in tTOblobdic.keys():
#                             continue
#                         if blob in blobTOtdic.keys():
#                             continue
                        
#                         tTOblobdic[layer_id] = blob
#                         blobTOtdic[blob] = layer_id

#                         size = os.stat(blob).st_size
                
                        r = {
                            "host": request['host'],
                            "http.request.duration": request['http.request.duration'],
                            "http.request.method": request['http.request.method'],
                            "http.request.remoteaddr": request['http.request.remoteaddr'],
                            "http.request.uri": request['http.request.uri'],
                            "http.request.useragent": request['http.request.useragent'],
                            "http.response.status": request['http.response.status'],
                            "http.response.written": size,
                            "id": request['id'],
                            "timestamp": request['timestamp']
#                             'data': blob
                        }
                        print r
                        ret.append(r)
#                         i += 1
#     return ret 
    ret.sort(key= lambda x: x['timestamp'])                          
    with open(os.path.join(input_dir, 'total_trace.json'), 'w') as fp:
        json.dump(ret, fp)      
        

def analyze_layerlifetime():
    
    layerPUTGAcctimedic = defaultdict(list)
    layerNPUTGAcctimedic = defaultdict(list)
    layerNGETAcctimedic = {}
    
    layer1stPUT = -1
#     layer1stGET = -1
    layerNPUT = False
    layerNGET = False
#     layecntGET = 0
    
    with open(os.path.join(input_dir, 'layer_access.json')) as fp:
        layerTOtimedic = json.load(fp)

    for k in sorted(layerTOtimedic, key=lambda k: len(layerTOtimedic[k]), reverse=True):
         
        #lifetime = layerTOtimedic[k][len(layerTOtimedic[k][0])-1][1] - layerTOtimedic[k][0][1]
        #lifetime = lifetime.total_seconds()
           
        lst = layerTOtimedic[k]
        
        if 'PUT' == lst[0][0]:
            layer1stPUT = 0
            if len(lst) == 1:
                layerNGET = True
        else:
            layerNPUT = True
            
        if True == layerNGET:
            layerNGETAcctimedic[k] = True
            continue
        
        starttime = lst[0][1]#datetime.datetime.strptime(lst[0][1], '%Y-%m-%dT%H:%M:%S.%fZ')
        interaccess = ((starttime),)
        #interaccess = interaccess + (k,)
        # (digest, next pull time)
        
        for i in range(len(lst)-1):
            nowtime = datetime.datetime.strptime(lst[i][1], '%Y-%m-%dT%H:%M:%S.%fZ')#lst[i][1]
            nexttime = datetime.datetime.strptime(lst[i+1][1], '%Y-%m-%dT%H:%M:%S.%fZ')#lst[i+1][1]
            delta = nexttime - nowtime
            delta = delta.total_seconds()
            
            interaccess = interaccess + (delta,)                   
                    
        if -1 == layer1stPUT:
            layerNPUTGAcctimedic[k] = interaccess
        else:
            layerPUTGAcctimedic[k] = interaccess

    if layerNGETAcctimedic:
        with open(os.path.join(input_dir, 'layerNGETAcctime.json'), 'w') as fp:
            json.dump(layerNGETAcctimedic, fp)
    if layerNPUTGAcctimedic:
        with open(os.path.join(input_dir, 'layerNPUTAcctime.json'), 'w') as fp:
            json.dump(layerNPUTGAcctimedic, fp)
    if layerPUTGAcctimedic:
         with open(os.path.join(input_dir, 'layerPUTGAcctime.json'), 'w') as fp:
             json.dump(layerPUTGAcctimedic, fp)


def analyze_requests(total_trace):
    organized = []
    layerTOtimedic = defaultdict(list)
    
#     start = ()

#     if round_robin is False:
#         ring = hash_ring.HashRing(range(numclients))
    with open(total_trace, 'r') as f:
        blob = json.load(f)

#     for i in range(numclients):
#         organized.append([{'port': port, 'id': random.getrandbits(32), 'threads': client_threads, 'wait': wait, 'registry': registries, 'random': push_rand}])
#         print organized[-1][0]['id']
#     i = 0

    for r in blob:
        uri = r['http.request.uri']
        usrname = uri.split('/')[1]
        repo_name = uri.split('/')[2]
        
        if 'blob' in uri:
            # uri format: v2/<username>/<repo name>/[blobs/uploads | manifests]/<manifest or layer id>
            layer_id = uri.rsplit('/', 1)[1]
            timestamp = r['timestamp']
    #        timestamp = datetime.datetime.strptime(r['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            method = r['http.request.method']
        
            print "layer_id: "+layer_id
            print "repo_name: "+repo_name
            print "usrname: "+usrname
            
    #         if layer_id in layerTOtimedic.keys():
            layerTOtimedic[layer_id].append((method, timestamp))
        
    with open(os.path.join(input_dir, 'layer_access.json'), 'w') as fp:
        json.dump(layerTOtimedic, fp)
        

def analyze_repo_reqs(total_trace):
#     organized = []
    usrTOrepoTOlayerdic = defaultdict(list) # repoTOlayerdic
    repoTOlayerdic = defaultdict(list)
    usrTOrepodic = defaultdict(list) # repoTOlayerdic
    
#     start = ()

#     if round_robin is False:
#         ring = hash_ring.HashRing(range(numclients))
    with open(total_trace, 'r') as f:
        blob = json.load(f)

#     for i in range(numclients):
#         organized.append([{'port': port, 'id': random.getrandbits(32), 'threads': client_threads, 'wait': wait, 'registry': registries, 'random': push_rand}])
#         print organized[-1][0]['id']
#     i = 0

    # get usr -> repo -> layer map

    for r in blob:
        uri = r['http.request.uri']
        usrname = uri.split('/')[1]
        repo_name = uri.split('/')[2]
        repo_name = usrname+'/'+repo_name
        
        if 'blob' in uri:
            layer_id = uri.rsplit('/', 1)[1]
            timestamp = r['timestamp']
    #        timestamp = datetime.datetime.strptime(r['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            method = r['http.request.method']
        
            print "layer_id: "+layer_id
            print "repo_name: "+repo_name
            print "usrname: "+usrname
            
            if repo_name in repoTOlayerdic.keys():
                if layer_id not in repoTOlayerdic[repo_name]:
                    repoTOlayerdic[repo_name].append(layer_id)
            else:
                repoTOlayerdic[repo_name].append(layer_id)
                
            
#             try:
#                 lst = repoTOlayerdic[repo_name]
#                 if layer_id not in lst:
#                     repoTOlayerdic[repo_name].append(layer_id)
#             except Exception as e:
#                 print "repo has not this layer before"
#                 repoTOlayerdic[repo_name].append(layer_id)

            #if 
                
            try:
                lst = usrTOrepodic[usrname]
                if repo_name not in lst:
                    usrTOrepodic[usrname].append(repo_name)
            except Exception as e:
                print "usrname has not this repo before"
                usrTOrepodic[usrname].append(repo_name)
#             if layer_id
            
    #         if layer_id in layerTOtimedic.keys():
            #layerTOtimedic[layer_id].append((method, timestamp))
#             repoTOlayerdic[repo_name].append(layer_id)

    for repo in repoTOlayerdic.keys():
        jsondata = {
            ''
        }

    for usr in usrTOrepodic.keys():
        for repo in usrTOrepodic[usr]:
            usrTOrepoTOlayerdic[usr].append({repo:repoTOlayerdic[repo]})
            
    for usr in usrTOrepodic.keys():
        jsondata = {
            'usr': usr,
            'repos': usrTOrepoTOlayerdic[usr]
            
        }
        
    with open(os.path.join(input_dir, 'usr2repo2layer_map.json'), 'w') as fp:
        json.dump(usrTOrepoTOlayerdic, fp)


def analyze_usr_repolifetime():
#     layerPUTGAcctimedic = defaultdict(list)  # 
#     layerNPUTGAcctimedic = defaultdict(list) #
#     layerNGETAcctimedic = {}
    
#     layer1stPUT = -1
# #     layer1stGET = -1
#     layerNPUT = False
#     layerNGET = False
#     layecntGET = 0
    
    with open(os.path.join(input_dir, 'usr2repo2layer_map.json')) as fp:
        usrrepolayer_map = json.load(fp)

#     with open(os.path.join(input_dir, 'layer_access.json')) as fp:
#         layerTOtimedic = json.load(fp)

#     for k in sorted(layerTOtimedic, key=lambda k: len(layerTOtimedic[k]), reversed=True):
#          
#         lifetime = layerTOtimedic[k][len(layerTOtimedic[k][0])-1][1] - layerTOtimedic[k][0][1]
#         lifetime = lifetime.total_seconds()
#            
#         lst = layerTOtimedic[k]
#         
#         if 'PUT' == lst[0][0]:
#             layer1stPUT = 0
#             if len(lst) == 1:
#                 layerNGET = True
#         else:
#             layerNPUT = True
#             
#         if False == layerNGET:
#             layerNGETAcctimedic[k] = True
#             continue
#         
#         interaccess = ((),)
#         #interaccess = interaccess + (k,)
#         # (digest, next pull time)
#         
#         for i in len(lst)-2:
#             nowtime = lst[i][1]
#             nexttime = lst[i+1][1]
#             delta = nexttime - nowtime
#             delta = delta.total_seconds()
#             
#             interaccess = interaccess + (delta,)                   
#                     
#         if -1 == layer1stPUT:
#             layerNPUTGAcctimedic[k] = interaccess
#         else:
#             layerPUTGAcctimedic[k] = interaccess

#     if layerNGETAcctimedic:

    NlayerPUTGAcctimedic = 0
    NlayerNPUTGAcctimedic = 0
    NlayerNGETAcctimedic = 0
    
    repoTOlayerdic = {} #defaultdict(list)
    
#     userTOlayerdic = defaultdict(list)

    with open(os.path.join(input_dir, 'layerNGETAcctime.json')) as fp1:
        layerNGETAcctimedic = json.load(fp1)
#     if layerNPUTGAcctimedic:
    with open(os.path.join(input_dir, 'layerNPUTAcctime.json')) as fp2:
        layerNPUTGAcctimedic = json.load(fp2)
#     if layerPUTGAcctimedic:
    with open(os.path.join(input_dir, 'layerPUTGAcctime.json')) as fp3:
         layerPUTGAcctimedic = json.load(fp3)
    #cnt = 0
    for usr in usrrepolayer_map.keys():   
        #cnt += 1
        #if cnt > 100:
        #    break;     
#         usrTOrepodic = defaultdict(list) # repoTOlayerdic
#         repoTONPUTAlayerdic = defaultdict(list)
#         repoTONGETAlayerdic = defaultdict(list)
        print "process usr "+usr
        for repo_item in usrrepolayer_map[usr]:  
            for repo, layers in repo_item.items():                         
                if repo in repoTOlayerdic.keys():
			continue
                print "process repo "+repo
                #cnt += 1
		#if cnt > 1000:
		   
                #repoTOPUTGAlayerdic = defaultdict(list) # repoTOlayerdic
                #repoTONPUTAlayerdic = defaultdict(list)
                #repoTONGETAlayerdic = defaultdict(list)
                
#                 repodic = defaultdict(list)
                repodic = {
                        'layerPUTGAlayerdic': [],
                        'layerNPUTGAcctimedic': [],
                        'layerNGETAlayerdic': []
                    }
            
                for layer in layers:#repo.keys():
                    if layer in layerPUTGAcctimedic.keys():
                        NlayerPUTGAcctimedic = 1
                        lst = layerPUTGAcctimedic[layer]
                    elif layer in layerNPUTGAcctimedic.keys():
                        NlayerNPUTGAcctimedic = 1
                        lst = layerNPUTGAcctimedic[layer]
                    elif layer in layerNGETAcctimedic.keys():
                        NlayerNGETAcctimedic = 1
                        lst = layerNGETAcctimedic[layer]
                    else:
                        print "cannot find the layer: "+layer 
                        continue                            
                    
#                     if NlayerPUTGAcctimedic and NlayerNPUTGAcctimedic and NlayerNGETAcctimedic:
#                         print "this is not a legal layer"
#                         continue
                    if NlayerPUTGAcctimedic == 1:
                        print "this is a layerPUTGAcctimedic "+layer
#                         repoTOPUTGAlayerdic[layer].append(lst)
                        repodic['layerPUTGAlayerdic'].append({layer: lst}) 
                    elif NlayerNPUTGAcctimedic == 1:
                        print "this is a layerNPUTGAcctimedic "+layer
#                         repoTONPUTAlayerdic[layer].append(lst)
                        repodic['layerNPUTGAcctimedic'].append({layer: lst})
                    elif NlayerNGETAcctimedic == 1:
                        print "this is a layerNGETAcctimedic "+layer
#                         repoTONGETAlayerdic[layer].append(lst)
                        repodic['layerNGETAlayerdic'].append({layer: lst})
                        
            repoTOlayerdic[repo] = repodic
        
#             repoTOlayerdic[repo]['repoTOPUTGAlayerdic'] =  repodic['repoTOPUTGAlayerdic']
#             repoTOlayerdic[repo]['repoTONPUTAlayerdic'] =  repodic['repoTONPUTAlayerdic']
#             repoTOlayerdic[repo]['repoTONGETAlayerdic'] =  repodic['repoTONGETAlayerdic']
            
    with open(os.path.join(input_dir, 'repo2layersaccesstime.json'), 'w') as fp:
        json.dump(repoTOlayerdic, fp)           
                    
         
                              
# tub = (k, lifetime)      
            
#             else:
#                 
#                 
#         
#         
#         else:
            
#                             continue
#         request = {
#             'delay': r['delay'],
#             'duration': r['duration'],
#             'data': r['data']
#         }

    
#         if r['uri'] in blob:
#             b = blob[r['uri']]
#             if b != 'bad':
#                 request['blob'] = b
#                 request['method'] = 'GET'
#                 if round_robin is True:
#                     organized[i % numclients].append(request)
#                     i += 1
#                 else:
#                     organized[ring.get_node(r['client'])].append(request)
#         else:
#             request['size'] = r['size']
#             request['method'] = 'PUT'
#             if round_robin is True:
#                 organized[i % numclients].append(request)
#                 i += 1
#             else:
#                 organized[ring.get_node(r['client'])].append(request)

#     return layerTOtimedic


def main():

    parser = ArgumentParser(description='Trace Player, allows for anonymized traces to be replayed to a registry, or for caching and prefecting simulations.')
    parser.add_argument('-i', '--input', dest='input', type=str, required=True, help = 'Input YAML configuration file, should contain all the inputs requried for processing')
    parser.add_argument('-c', '--command', dest='command', type=str, required=True, help= 'Trace player command. Possible commands: warmup, run, simulate, warmup is used to populate the registry with the layers of the trace, run replays the trace, and simulate is used to test different caching and prefecting policies.')

    args = parser.parse_args()
    
    trace_dir = input_dir+args.input
    
    print "input dir: "+trace_dir

    #NANNAN
    if args.command == 'get':    
#         if 'realblobs' in inputs['client_info']:
            #if inputs['client_info']['realblobs'] is True:
#             realblob_locations = inputs['client_info']['realblobs']
        get_requests(trace_dir)
        return
	    #else:
		#print "please put realblobs!"
		#return
    elif args.command == 'Alayer':
#         print "wrong cmd!"
        analyze_requests(os.path.join(input_dir, 'total_trace.json'))
        return 
    elif args.command == 'layerlifetime':
        analyze_layerlifetime()
        return
    elif args.command == 'map':
        analyze_repo_reqs(os.path.join(input_dir, 'total_trace.json'))
        return
    elif args.command == 'repolayer':
        analyze_usr_repolifetime()
        return
# 
        
    elif args.command == 'simulate':
#         if verbose:
#             print 'simulate mode'
#         if 'simulate' not in inputs:
#             print 'simulate file required in config'
#             exit(1)
#         pi = inputs['simulate']['name']
#         if '.py' in pi:
#             pi = pi[:-3]
        with open(os.path.join(input_dir, 'total_trace.json'), 'r') as fp:
            json_data = json.load(fp)
        
        pi = 'prefetch_old' #'cache_usr_repo_layer'
        try:
            plugin = importlib.import_module(pi)
        except Exception as inst:
            print 'Plugin did not work!'
            print inst
            exit(1)
        try:
            plugin.init(json_data)
        except Exception as inst:
            print 'Error running plugin init!'
            print inst
            print traceback.print_exc()

if __name__ == "__main__":
    main()
