# -*- Mode: Python; -*-

import io, glob, time

class objref:
    def __init__(self,rep):
        self.rep = rep

class oidref:
    def __init__(self,ref):
        if ref[0] == '@':
            hilo = ref.split('/')
            self.hi = hi
            self.lo = lo

def read_item(string):
    if string[0].isdigit():
        try:
            return int(string)
        except:
            return objref(string)
    else:
        return objref(string)

def read_datafile(infile):
    items = set()
    inport = io.open(infile)
    line = inport.readline()
    while len(line) > 0:
        items.add(read_item(line))
        line = inport.readline()
    return items

def get_data_files(prefix):
    files = []
    return files

def read_data(prefix):
    table = {}
    files = glob.glob(prefix+'*')
    for file in files:
        table[file] = read_datafile(file)
    return table

def time_fcn(fn,arg=None):
    start=time.clock()
    if arg is None:
        result=fn()
    else:
        result=fn(arg)
    end=time.clock()
    return end-start    

def closest_pairs(data):
    started = time.clock()
    best = set()
    best_score = None
    for each in data:
        for other in data:
            if each != other:
                common = data[each].intersection(data[other])
                score = len(common)
                if best_score == None:
                    best_score = score
                    best.add(each+" - "+other)
                elif score == best_score:
                    best.add(each+" - "+other)
                elif score > best_score:
                    best_score = score
                    best = set()
                    best.add(each+" - "+other)
    return {'count': len(best), 'score': best_score, 'best': best, 'time': time.clock()-started}

data = {}

def read_oid_data(prefix='setbench/oids/x'):
    data[prefix] = read_data(prefix)
