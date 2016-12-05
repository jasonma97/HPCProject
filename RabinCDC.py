'''
This is a python implementation of FastCDC.
Author: Kanishk Tantia, Jonathan Cruz
Date:  June 7th, 2016

Pseudocode:
Input: data buffer, src; buffer length, n
Output: chunking breakpoint i
Macro Defined: Mask <-- 0x7
Macro Defined: MinSize <-- 2KB; MaxSize <-- 64KB;
    fp <-- 0; i <-- MinSize; NormalSize <-- 8KB;
if n <= MinSize then
    return n;
if n >= MaxSize then
    n <-- MaxSize;
else if n <= NormalSize then
    n <-- NormalSize;
for ; i < n; i++; do
    fp = (fp << 1) + Gear[src[i]];
    if ! (fp & Mask) then
        return i; // if the masked bits are all '0'
return i;
'''
import sys
from multiprocessing import Pool
from time import time
numProcs  = 4


def string2numeric_hash(text):
    import hashlib
    return int(hashlib.md5(text).hexdigest()[:8], 16)

def RabinCDC(LBAlist):
    P = 53424
    a = 10
    rolling_hash = 0
    n = len(LBAlist)
    breakindices = []
    for i in range(a,n-1):
        rolling_hash += sum(LBAlist[i-a:i])
        secondhash = string2numeric_hash(str(rolling_hash).encode('utf-8'))
        if secondhash%9 == 0:
            breakindices.append(i)
            rolling_hash = 0
    return breakindices



def main(src):
    '''
    Input: data buffer src
    '''
    target = open(src, 'r', encoding='utf-8')

    LBAlist = target.readlines()
    LBAlist = [int(x) for x in LBAlist]

    breakindices = RabinCDC(LBAlist)

    outputfile = open("RabinOut", 'w')
    for i in breakindices:
        outputfile.write("%s\n" % i)
    outputfile.close()



def serialCode(LBAlist):
    '''
    Input: data buffer src
    '''

    breakindices = RabinCDC(LBAlist)
    #print(breakindices)
    #print(len(breakindices))
    outputfile = open("RabinOut", 'w')
    for i in breakindices:
        outputfile.write("%s\n" % i)
    outputfile.close()


def timeTrials(src):
    target = open(src, 'r')
    global LBAlist
    LBAlist = target.readlines()
    LBAlist = [int(x) for x in LBAlist]



    start = time()
    serialCode(LBAlist)
    t = time() - start
    print(t)
    time_list = []
    global numProcs

    #MultiProcessor Run
    for i in range(2, numProcs + 1):
        start = time()
        chunkSize = int(len(LBAlist)/numProcs)
        jobs = []
        for p in range(0,i):
            if p == 0:
                jobs.append((LBAlist[:chunkSize]))
            elif p != i-1 and p > 0:
                jobs.append((LBAlist[chunkSize*p:chunkSize*(p+1)]))
            else:
                jobs.append((LBAlist[chunkSize*p:]))
        pool = Pool(i).map(RabinCDC, jobs)
        newPool = []
        for i in pool:
            newPool += i
        #print(newPool)
        #print(len(newPool))

        t2 = time() - start
        print(t2)
        time_list.append([i, t2])


if __name__ == '__main__':
    if len(sys.argv) == 2:
        src = sys.argv[1] # data buffer
        timeTrials(src)
    elif len(sys.argv) == 1:
        timeTrials('homes1')
    else:
        print("Usage: fastcdc.py <databuffer>")