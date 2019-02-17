import time
import pdb
import datetime
from lru import LRU
import json

type = 'layer'
# num_usrs = 9997
# num_repos = 40264
# num_layers = 829202

    
class complex_cache:
    def __init__(self, size, type): # the number of items
        self.size = size # actual size of the cache
        self.lru = LRU(size)

        self.hits = 0.0
        self.reqs = 0.0
        self.cache_stack_size = 0 # how much of the cache is occupied


    def place(self, request):
        # request is a tuple (timestamp, username)
        self.reqs += 1 
        if self.lru.has_key(request[-1]): 
            self.lru[request[-1]] = self.lru[request[-1]] + 1
            
            self.hits += 1            
        else:
            if self.cache_stack_size + 1 > self.size: 
                print "evict an item: "+str(self.lru.peek_last_item())
                self.cache_stack_size -= 1
                
            self.lru[request[-1]] = 1
            self.cache_stack_size += 1
            

def reformat(indata, type):
    ret = []
    print "reformating: wait 2 hrs ..." 
    for item in indata: 
	#print item
	uri = item['http.request.uri']
	timestamp = item['timestamp']       
        usrname = uri.split('/')[1]
        repo_name = uri.split('/')[2]
        repo_name = usrname+'/'+repo_name
        if type == 'layer':
            if 'manifests' in uri:
                continue
            layer = uri.split('/')[-1]
            ret.append((timestamp, layer)) # delay: datetime
            
        elif type == 'repo':
            ret.append((timestamp, repo_name)) # delay: datetime
        elif type == 'usr':
	    #print timestamp+','+usrname
            ret.append((timestamp, usrname)) # delay: datetime

    return ret

# 5%, 10%, 15%, 20%, 25%, 30%, 40%, 50%
# usrs: 9,997 
# repos: 40,264
# layers: 829,202

def run_sim(requests, type, portion):
    num_layers = {10: 194007,
                  25: 267151,
                  50: 339813,
                  75: 394729,
                  100: 450944
                  }
    t = time.time()
       
    # we are considering an individual layer size to be 1 unit.
    size1 = int(num_layers[portion] * 0.05)
    size2 = int(num_layers[portion] * 0.1)
    size3 = int(num_layers[portion] * 0.15)
    size4 = int(num_layers[portion] * 0.2)
    size5 = int(num_layers[portion] * 0.3)
    
    caches = []
    caches.append(complex_cache(size1, type))
    caches.append(complex_cache(size2, type))
    caches.append(complex_cache(size3, type))
    caches.append(complex_cache(size4, type))
    caches.append(complex_cache(size5, type))
    
    i = 0
    count = 10
    j = 0
    hr_no = 0
    hit_ratio_each_hr = {}
    
    for request in requests:
        j += 1
        if j == 1:
            starttime = request[0]
            print "starttime: "+str(starttime)
	#timestamp = datetime.datetime.strptime(request['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
	curtime= datetime.datetime.strptime(request[0], '%Y-%m-%dT%H:%M:%S.%fZ')
	initime = datetime.datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%S.%fZ')
	interval = int((curtime - initime).total_seconds()) #% (60*60)
        for c in caches:
            c.place(request)
        #if interval > 1 and interval % (60 * 60) == 0:
            #pdb breakpoint here
            #pdb.set_trace()
        if interval > 1 and interval % (60*60) == 0: # calculate for each hr
            hr_no = interval/(60*60)
            
            hit_ratio_each_hr[str(hr_no) + ' 5% hit ratio'] = caches[0].hits/caches[0].reqs
            hit_ratio_each_hr[str(hr_no) + ' 10% hit ratio'] = caches[1].hits/caches[1].reqs
            hit_ratio_each_hr[str(hr_no) + ' 15% hit ratio'] = caches[2].hits/caches[2].reqs
            hit_ratio_each_hr[str(hr_no) + ' 20% hit ratio'] = caches[3].hits/caches[3].reqs
            hit_ratio_each_hr[str(hr_no) + ' 30% hit ratio'] = caches[4].hits/caches[4].reqs
            
            print "5% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 5% hit ratio'])
            print "10% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 10% hit ratio'])
            print "15% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 15% hit ratio'])
            print "20% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 20% hit ratio'])
            print "30% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 30% hit ratio'])
                
        if 1.*i / len(requests) > 0.1:
            i = 0
            print str(count) + '% done'
            count += 10
        i += 1

    total_hit_ratio = {"5% cache": caches[0].hits/caches[0].reqs,
                       "10% cache": caches[1].hits/caches[1].reqs,
                       "15% cache": caches[2].hits/caches[2].reqs,
                       "20% cache": caches[3].hits/caches[3].reqs,
                       "30% cache": caches[4].hits/caches[4].reqs,
                       }

    return hit_ratio_each_hr, total_hit_ratio


def init(data, portion=100):

    print 'running cache simulation for: '+type
    #print data
    outputfile = 'hit_ratio_'+type+'_'+str(portion)+'_percent'+'.json'
    parsed_data = reformat(data, type)
    info, total_hit_ratio = run_sim(parsed_data, type, portion)
    
    with open(outputfile, 'w') as fp:
        json.dump(info, fp)

    with open("LRU_total_hit_ratios.json", 'a') as fp:
        fp.write(str(portion)+"% trace\n")
        json.dump(total_hit_ratio, fp)
        fp.write("\n\n")

#     for thing in info:
#         print thing + ': ' + str(info[thing])

