'''
This is a python implementation of FastCDC.
Author: Kanishk Tantia, Jason Ma
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
import multiprocessing
from time import time
import threading
global numProcs
numProcs = 1
numTests = 5
global LBAlist
LBAlist = []




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

def globalRabinCDC(b, n, q):
    P = 53424
    a = 10
    rolling_hash = 0
    global LBAlist
    breakindices = []
    for i in range(a + b,n-1):
        rolling_hash += sum(LBAlist[i-a:i])
        secondhash = string2numeric_hash(str(rolling_hash).encode('utf-8'))
        if secondhash%9 == 0:
            breakindices.append(i)
            rolling_hash = 0
    q.put(breakindices)

def RabinCDCParallel(LBAlist, q):
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
    q.put(breakindices)

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
    return breakindices

def serialTest(src, numTests):
    avgTime = []
    
    target = open(src, 'r')
    LBAlist = target.readlines()
    LBAlist = [int(x) for x in LBAlist]
    for i in range(numTests):
        start = time()
        serialResult = serialCode(LBAlist)
        t = time() - start
        #print(t)
        print("Time for 1 Processors: {0}".format(t))
        avgTime.append(t)

    print("Average Serial Runtime: {0}".format(sum(avgTime)/len(avgTime)))
    return avgTime

def testRabinParallel(src, numProcs, numTests):
    print("Testing Source File: " + str(src))
    target = open(src, 'r')
    if numProcs == 1:
        return serialTest(src, numTests)

    global LBAlist
    LBAlist = target.readlines()
    LBAlist = [int(x) for x in LBAlist]
    avgTime = []

    for i in range(numTest):
        start = time()
        chunkSize = int(len(LBAlist)/numProcs)
        listq = []

        for p in range(0,numProcs):
            q =  multiprocessing.Queue()
            if p != i-1:
                proc = multiprocessing.Process(target = globalRabinCDC, args = (chunkSize*p,chunkSize*(p+1),q))
            else:
                proc = multiprocessing.Process(target = globalRabinCDC, args = (chunkSize*p, len(LBAlist),q))
            
            proc.start()
            jobs.append(proc)
            listq.append(q)
        resultL = []

        for que in listq:
            resultL += que.get()
        for proc in jobs:
            proc.join()
            proc.terminate()

        t2 = time() - start
        avgTime.append(t)
        print("Time for {0} Processors: {1}".format(i, t))
    print("Average Runtime for {0} Processes: {1}".format(numProcs, sum(avgTime)/numTests))
    return avgTime

def testingSuite():
    
if __name__ == '__main__':
    if len(sys.argv) == 2:
        src = sys.argv[1] # data buffer
        multiprocessingTest(src)
    elif len(sys.argv) == 1:
        testRabinParallel('Data/homes1', 1, 1)
    else:
        print("Usage: fastcdc.py <databuffer>")











########################################################
#DEVELOPMENT HELL. WHERE DEPRACATED FUNCTIONS GO TO DIE#
########################################################
"""
def timeTrials(src):
    print("Testing Source File: " + str(src))
    target = open(src, 'r')
    global LBAlist
    LBAlist = target.readlines()
    LBAlist = [int(x) for x in LBAlist]
    global numProcs

    avgTime = [0 for x in range(numProcs )]

    for i in range(numTests):
        start = time()
        serialCode(LBAlist)
        t = time() - start
        #print(t)
        print("Time for 1 Processors: {0}".format(t))
        avgTime[0] = avgTime[0] + t/numTests
        #print(avgTime)

    #MultiProcessor Run
    for test in range(numTests):
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
            pool = multiprocessing.Pool(i).map(RabinCDC, jobs)
            # for process in pool:
            #     process.join()
            #     process.terminate()
            newPool = []
            
            for p in pool:
                newPool += p

            #print(newPool)
            #print(len(newPool))
            t2 = time() - start
            #print(t2)
            avgTime[i - 1] += t2/numTests
            print("Time for {0} Processors: {1}".format(i, t2))
    print(avgTime)
    resultFile = open('times.txt', 'a+')
    resultFile.write(src + '\n')
    for testTime in avgTime:
        resultFile.write(str(testTime) + " ")
    resultFile.write('\n')

"""


## COPIES ABOVE CODE, BUT NOW DOESN'T CREATE HUNDREDS OF PROCESSES
def multiprocessingTest(src):
    print("Testing Source File: " + str(src))
    target = open(src, 'r')
    global LBAlist
    LBAlist = target.readlines()
    LBAlist = [int(x) for x in LBAlist]

    global numProcs
    avgTime = [0 for x in range(numProcs)]

    # for i in range(numTests):
    #     start = time()
    #     serialResult = serialCode(LBAlist)
    #     t = time() - start
    #     #print(t)
    #     print("Time for 1 Processors: {0}".format(t))
    #     avgTime[0] = avgTime[0] + t/numTests
    # print(avgTime)
    #print(serialResult)
    #MultiProcessor Run
    for i in range(numProcs, numProcs + 1):
        for test in range(numTests):
            start = time()
            chunkSize = int(len(LBAlist)/numProcs)
            jobs = []
            listq = []
            for p in range(0,i):
                q =  multiprocessing.Queue()
                if p != i-1:
                    proc = multiprocessing.Process(target = globalRabinCDC, args = (chunkSize*p,chunkSize*(p+1),q))
                else:
                    proc = multiprocessing.Process(target = globalRabinCDC, args = (chunkSize*p, len(LBAlist),q))
                proc.start()
                jobs.append(proc)
                listq.append(q)
            #Process Synchronization
            resultL = []

            for que in listq:
                resultL += que.get()
            for proc in jobs:
                #print("Try")
                proc.join()
                #print("We got here")
                proc.terminate()
                #print("Hi")

            t2 = time() - start
            avgTime[i - 1] += t2/numTests
            print("Time for {0} Processors: {1}".format(i, t2))
        print(avgTime)
    resultFile = open('ProcessorTimesFinal.txt', 'a+')
    resultFile.write(src + '\n')
    for testTime in avgTime:
        resultFile.write(str(testTime) + " ")
    resultFile.write('\n')

def threadingTest(src):
    print("Testing Source File: " + str(src))
    target = open(src, 'r')
    global LBAlist
    LBAlist = target.readlines()
    LBAlist = [int(x) for x in LBAlist]

    global numProcs
    avgTime = [0 for x in range(numProcs )]

    for i in range(numTests):
        start = time()
        serialResult = serialCode(LBAlist)
        t = time() - start
        #print(t)
        print("Time for 1 Processors: {0}".format(t))
        avgTime[0] = avgTime[0] + t/numTests
    print(avgTime)
    #print(serialResult)
    #MultiProcessor Run
    for i in range(2, numProcs + 1):
        for test in range(numTests):
            start = time()
            chunkSize = int(len(LBAlist)/numProcs)
            jobs = []
            listq = []
            for p in range(0,i):
                if p != i-1:
                    thr = ThreadWithReturnValue(target = RabinCDC, args = [LBAlist[chunkSize*p:chunkSize*(p+1)]])
                else:
                    thr = ThreadWithReturnValue(target = RabinCDC, args = [LBAlist[chunkSize*p:]])
                thr.start()
                jobs.append(thr)
            #Process Synchronization
            resultL = []
            for thr in jobs:
                print("Try")
                #print("We got here")
                resultL += thr.join()
                #print("Hi")

            #print(resultL)
            #print(newPool)
            #print(len(newPool))
            t2 = time() - start
            #print(t2)
            avgTime[i - 1] += t2/numTests
            print("Time for {0} Threads: {1}".format(i, t2))
        print(avgTime)
    resultFile = open('threadingTimes.txt', 'a+')
    resultFile.write(src + '\n')
    for testTime in avgTime:
        resultFile.write(str(testTime) + " ")
    resultFile.write('\n')

def testSameProblemScale():
    print("Testing Problem Scalability")
    target = open('homestest1', 'r')
    global LBAlist
    LBAlist = target.readlines()
    LBAlist = [int(x) for x in LBAlist]

    global numProcs
    avgTime = [0 for x in range(numProcs )]

    for i in range(numTests):
        start = time()
        serialResult = serialCode(LBAlist)
        t = time() - start
        #print(t)
        print("Time for 1 Processors: {0}".format(t))
        avgTime[0] = avgTime[0] + t/numTests
    print(avgTime)
    #print(serialResult)
    #MultiProcessor Run
    for i in range(2, numProcs + 1):
        target = open('homestest' + str(i), 'r')
        LBAlist = target.readlines()
        LBAlist = [int(x) for x in LBAlist]
        print(len(LBAlist))
        for test in range(numTests):
            start = time()
            chunkSize = int(len(LBAlist)/numProcs)
            jobs = []
            listq = []
            for p in range(0,i):
                q =  multiprocessing.Queue()
                if p != i-1:
                    proc = multiprocessing.Process(target = globalRabinCDC, args = (chunkSize*p,chunkSize*(p+1),q))
                else:
                    proc = multiprocessing.Process(target = globalRabinCDC, args = (chunkSize*p, len(LBAlist),q))
                proc.start()
                jobs.append(proc)
                listq.append(q)
            #Process Synchronization
            resultL = []

            for que in listq:
                resultL += que.get()
            for proc in jobs:
                #print("Try")
                proc.join()
                #print("We got here")
                proc.terminate()
                #print("Hi")

            t2 = time() - start
            avgTime[i - 1] += t2/numTests
            print("Time for {0} Processors: {1}".format(i, t2))
        print(avgTime)

    avgSerialTime = [0 for x in range(numProcs)]
    for pNum in range(1,numProcs + 1):
        for i in range(numTests):
            target = open('homestest' + str(pNum), 'r')
            LBAlist = target.readlines()
            LBAlist = [int(x) for x in LBAlist]
            # print(len(LBAlist))
            start = time()
            serialResult = serialCode(LBAlist)
            t = time() - start
            #print(t)
            print("Serial Time: {0}".format(t))
            avgSerialTime[pNum - 1] = avgSerialTime[pNum - 1] + t/numTests
        print(avgSerialTime)



    resultFile = open('problemScaling.txt', 'a+')
    resultFile.write('Scaling Times' + '\n')
    for testTime in avgTime:
        resultFile.write(str(testTime) + " ")
    resultFile.write('\n')
    resultFile.write('Serial Times' + '\n')
    for testTime in avgSerialTime:
        resultFile.write(str(testTime) + " ")
    resultFile.write('\n')


if __name__ == '__main__':
    if len(sys.argv) == 2:
        src = sys.argv[1] # data buffer
        multiprocessingTest(src)
    elif len(sys.argv) == 3:
        src = sys.argv[1] # data buffer
        multiprocessingTest(src)
        numProcs = sys.argv[2]    
    elif len(sys.argv) == 1:
        # multiprocessingTest('allhomes')
        # for i in range(1, 19):
        #     try:
        #         multiprocessingTest('homes' + str(i))
        #     except:
        #         continue
        testSameProblemScale()

        #threadingTest('homes18')
        # for i in range(17, 18):
        #     try:
        #         testEverything('homes' + str(i))
        #     except:
        #         continue
    else:
        print("Usage: fastcdc.py <databuffer> <numProcs>")
