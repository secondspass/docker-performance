import time

class cache_region:
    def __init__(self, size, usrwindow, repowindow, layerwindow):
        self.size = size
        self.usrwindow = usrwindow
        self.repowindow = repowindow
        self.layerwindow = layerwindow
        
        self.usr_lru = []
        self.usr_map = {}
        
        self.repo_lru = []
        self.repo_map = {}
        
        self.layer_lru = []
        self.layer_map = {}
        
        self.evict_times = []
        
    def adjust_cache_stack(self, layer_id, reponame, usrname):
        
    def store_to_cache(self, layer_id, reponame, usrname):  
        
    def read_from_cache(self, layer_id, reponame, usrname): 
        
    def evict_from_cache(self, no_layers, no_repos, no_usrs): 


class prefetch_region:
    def __init__(self, size, prefectch_repo_window, prefetch_layer_window):
        self.size = size
        self.repowindow = prefectch_repo_window
        self.layerwindow = prefetch_layer_window
        
        self.usr_lru = []
        self.usr_map = {}
        
        self.repo_lru = []
        self.repo_map = {}
        
        self.layer_lru = []
        self.layer_map = {}
        
        self.evict_times = []
        
    def adjust_prefetch_stack(self, layer_id, reponame, usrname):

    def store_to_prefetch(self, layer_id, reponame, usrname):  
        
    def read_from_prefetch(self, layer_id, reponame, usrname):  
        
    def evict_from_prefetch(self, no_layers, no_repos, no_usrs):      

class fs:
    def __init__(self):
        self.usr_lru = []
        self.usr_map = {}
        
        self.repo_lru = []
        self.repo_map = {}
        
        self.layer_lru = []
        self.layer_map = {}
        
        self.evict_times = []

    def store_to_fs(self, layer_id, repo, usrname):  
        
    def read_from_fs(self, layer_id, repo, usrname): 

class adaptive_cache:
    #size=1., usrwindow=100, repowindow=10, layerwindow=20
    def __init__(self, size, cache_prefetch_ratio, usrwindow, repowindow, layerwindow, prefectch_repo_window, prefetch_layer_window):
        self.size = size
        self.cache_prefetch_ratio = cache_prefetch_ratio
        self.cache_region_size = 0
        self.prefetch_region_size = 0
        
        self.usrwindow = 0
        self.repowindow = 0
        self.layerwindow = 0
        
        self.prefetch_repo_window = 0
        self.prefetch_layer_window = 0
        
        self.whole_usr_lsr = [] #(position, arrive_time, layer, repo, usr)
        self.whole_repo_lsr = []
        self.whole_layer_lsr = []
#         self.whole_stack = []
        
    def init_cache_region(self, size * cache_prefetch_ratio, usrwindow, repowindow, layerwindow):
        
        
    def init_prefetch_region(self, size*(1-cache_prefetch_ratio), prefectch_repo_window, prefetch_layer_window):
               
        
    def adjust_cache_prefetch_ratio(self):

        
    def place(self, request):
        if request[-1] in self.mem:
            self.lmu.append(request[-1])
            self.mem[request[-1]][1] += 1
            self.hits += 1
            if self.first == True:
                self.firstmemhits += 1
            if self.fsfirst == True:
                self.firstfsmemhits += 1
            
        else:
            self.misses += 1
            if self.first == True:
                self.firstMiss += 1

            if request[1] >= self.restrict:
                self.fsPlace(request[-1], request[1])
                return

            self.fsCheck(request)

            if request[1] + self.size <= self.capacity:
                self.mem[request[-1]] = [request[1], 1]
                self.lmu.append(request[-1])
                self.size += request[1]
            else:
                self.first = False
                while request[1] + self.size > self.capacity:
                    eject = self.lmu.pop(0)
                    self.mem[eject][1] -= 1
                    if self.mem[eject][1] > 0:
                        continue
                    self.fsPlace(eject, self.mem[eject][0], ejected=True)
                    self.size -= self.mem[eject][0]
                    self.mem.pop(eject, None)
                    self.evictions += 1

                self.mem[request[-1]] = [request[1], 1]
                self.lmu.append(request[-1])
                self.size += request[1]




def reformat(indata):
    ret = []
    for item in indata:
        if 'manifest' in item['uri']:
            continue

        layer = item['uri'].split('/')[-1]
        ret.append((item['delay'], item['size'], layer)) # delay: datetime

    return ret

def run_sim(requests, size):
    t = time.time()
    caches = []
    caches.append(complex_cache(size=size, fssize = 10))
    caches.append(complex_cache(size=size, fssize = 15))
    caches.append(complex_cache(size=size, fssize = 20))
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
            hit_ratio_each_hr[str(hr_no) + ' 10 lmu hits'] = caches[i].get_lmu_hits()
            hit_ratio_each_hr[str(hr_no) + ' 10 lmu misses'] = caches[i].get_lmu_misses()
            hit_ratio_each_hr[str(hr_no) + ' 10 h hits'] = caches[i].get_h_hits()
            hit_ratio_each_hr[str(hr_no) + ' 10 h misses'] = caches[i].get_h_misses()
            hit_ratio_each_hr[str(hr_no) + ' 15 lmu hits'] = caches[i + 1].get_lmu_hits()
            hit_ratio_each_hr[str(hr_no) + ' 15 lmu misses'] = caches[i + 1].get_lmu_misses()
            hit_ratio_each_hr[str(hr_no) + ' 15 h hits'] = caches[i + 1].get_h_hits()
            hit_ratio_each_hr[str(hr_no) + ' 15 h misses'] = caches[i + 1].get_h_misses()
            hit_ratio_each_hr[str(hr_no) + ' 20 lmu hits'] = caches[i + 2].get_lmu_hits()
            hit_ratio_each_hr[str(hr_no) + ' 20 lmu misses'] = caches[i + 2].get_lmu_misses()
            hit_ratio_each_hr[str(hr_no) + ' 20 h hits'] = caches[i + 2].get_h_hits()
            hit_ratio_each_hr[str(hr_no) + ' 20 h misses'] = caches[i + 2].get_h_misses()
                
        if 1.*i / len(requests) > 0.1:
            i = 0
            print str(count) + '% done'
            count += 10
        for c in caches:
            c.place(request)
        i += 1
    hit_ratios = {}
    i = 0
    hit_ratios[str(i) + ' 10 lmu hits'] = caches[i].get_lmu_hits()
    hit_ratios[str(i) + ' 10 lmu misses'] = caches[i].get_lmu_misses()
    hit_ratios[str(i) + ' 10 h hits'] = caches[i].get_h_hits()
    hit_ratios[str(i) + ' 10 h misses'] = caches[i].get_h_misses()
    hit_ratios[str(i) + ' 15 lmu hits'] = caches[i + 1].get_lmu_hits()
    hit_ratios[str(i) + ' 15 lmu misses'] = caches[i + 1].get_lmu_misses()
    hit_ratios[str(i) + ' 15 h hits'] = caches[i + 1].get_h_hits()
    hit_ratios[str(i) + ' 15 h misses'] = caches[i + 1].get_h_misses()
    hit_ratios[str(i) + ' 20 lmu hits'] = caches[i + 2].get_lmu_hits()
    hit_ratios[str(i) + ' 20 lmu misses'] = caches[i + 2].get_lmu_misses()
    hit_ratios[str(i) + ' 20 h hits'] = caches[i + 2].get_h_hits()
    hit_ratios[str(i) + ' 20 h misses'] = caches[i + 2].get_h_misses()

    return hit_ratios

def init(data, args):
    cache_size = args['cache_size']

    print 'running cache simulation'

    parsed_data = reformat(data)

    info = run_sim(parsed_data, cache_size)

    for thing in info:
        print thing + ': ' + str(info[thing])

