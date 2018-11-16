import time
from lru import LRU
import json

type = 'usr'
num_usrs = 9997
num_repos = 40264
num_layers = 829202

outputfile = 'hit_ratio_'+type+'.json'
    
class complex_cache:
    def __init__(self, size, type): # the number of items
        self.size = size
        self.lru = LRU(size)

        self.hits = 0
        self.reqs = 0
        self.cache_stack_size = 0


    def place(self, request):
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
     
    for item in indata:        
        usrname = item['uri'].split('/')[1]
        repo_name = item['uri'].split('/')[2]
        repo_name = usrname+'/'+repo_name
        if type == 'layer':
            if 'manifest' in item['uri']:
                continue
            layer = item['uri'].split('/')[-1]
            ret.append((item['delay'], layer)) # delay: datetime
            
        elif type == 'repo':
            ret.append((item['delay'], repo_name)) # delay: datetime
        elif type == 'usr'
            ret.append((item['delay'], usrname)) # delay: datetime

    return ret

# 5%, 10%, 15%, 20%, 25%, 30%, 40%, 50%
# usrs: 9,997 
# repos: 40,264
# layers: 829,202

def run_sim(requests):
    t = time.time()
       
    size1 = int(num_usrs * 0.05)
    size2 = int(num_usrs * 0.1)
    size3 = int(num_usrs * 0.15)
    size4 = int(num_usrs * 0.2)
    
    caches = []
    caches.append(complex_cache(size=size1, type='usr'))
    caches.append(complex_cache(size=size2, type='usr'))
    caches.append(complex_cache(size=size3, type='usr'))
    caches.append(complex_cache(size=size4, type='usr'))
    
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
        if int((request[j] - starttime).total_seconds) % (60*60) == 0: # calculate for each hr
            hr_no += 1
            
            hit_ratio_each_hr[str(hr_no) + ' 5% hit ratio'] = caches[0].hits/caches[0].reqs
            hit_ratio_each_hr[str(hr_no) + ' 10% hit ratio'] = caches[1].hits/caches[1].reqs
            hit_ratio_each_hr[str(hr_no) + ' 15% hit ratio'] = caches[2].hits/caches[2].reqs
            hit_ratio_each_hr[str(hr_no) + ' 20% hit ratio'] = caches[3].hits/caches[3].reqs
            
            print "5% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 5% hit ratio'])
            print "10% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 10% hit ratio'])
            print "15% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 15% hit ratio'])
            print "20% hit ratio"+str(hit_ratio_each_hr[str(hr_no) + ' 20% hit ratio'])
                
        if 1.*i / len(requests) > 0.1:
            i = 0
            print str(count) + '% done'
            count += 10
        for c in caches:
            c.place(request)
        i += 1

    return hit_ratio_each_hr


def init(data):

    print 'running cache simulation'

    parsed_data = reformat(data, type)

    info = run_sim(parsed_data, type)
    
    with open(outputfile, 'w') as fp:
        json.dump(info, fp)

#     for thing in info:
#         print thing + ': ' + str(info[thing])

