import csv
import itertools
import numpy

def readdata(filepath):
    with open(filepath, 'r') as fin:
        reader = csv.DictReader(fin.read().split('\n'))
    return reader
        
def get(fieldname):
    """
    """
    def _get(iterable):
        result = []
        for item in iterable:
            result.append(float(item[fieldname]))
        return result
    return _get

def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]

def avg(data, chunksize):
    slices = chunks(list(data), chunksize)
    
    getmaxtemp = get('maxtemp')
    getmintemp = get('mintemp')
    
    min_mint = map(numpy.min,   map(getmintemp, slices))
    avg_mint = map(numpy.mean, map(getmintemp, slices))
    max_mint = map(numpy.max,   map(getmintemp, slices))
    
    min_maxt = map(numpy.min,   map(getmaxtemp, slices))
    avg_maxt = map(numpy.mean,  map(getmaxtemp, slices))
    max_maxt = map(numpy.max,   map(getmaxtemp, slices))
    
    
    min_mint_r = map(int, min_mint)
    avg_mint_r = map(int, avg_mint)
    max_mint_r = map(int, max_mint)
                          
    min_maxt_r = map(int, min_maxt)
    avg_maxt_r = map(int, avg_maxt)
    max_maxt_r = map(int, max_maxt)
    
    
    result = zip(range(1, 1+len(slices)), 
                 min_mint_r,
                 avg_mint_r,
                 max_mint_r,
                           
                 min_maxt_r,
                 avg_maxt_r,
                 max_maxt_r)
    return result

if __name__ == '__main__':
    w12data = readdata('w12.csv')
    stats = avg(w12data, chunksize = 30)
    print stats
    with open('w12processed.csv', 'w+') as fout:
        writer = csv.writer(fout)
        writer.writerows(stats)
