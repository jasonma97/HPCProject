import random
import sys
import time
def genDataset(src, N, newDataSize):
    random.seed(time.time())
    lines = open(src, 'r+').readlines()
    setOfSets = []
    for a in range(N + 1):
        setOfSets.append([])
    for i in range(len(lines)):
        for b in range(2,N):
            newSet = lines[i:i + b]
            if newSet not in setOfSets[b]:
                setOfSets[b].append(newSet)

    count = 0
    f = open('dummy.txt', 'w+')
    num = [0 for x in range(N)]
    while (count < newDataSize):
        size = int(random.random() * (N + 1))
        num[size] += 1
        if size > 0 and size < N:
            writeData = random.choice(setOfSets[size])
            for data in writeData:
                f.write(str(data))
            count += size
        elif size == 0:
            writeData = random.choice(lines)
            print("yo")
            f.write(str(writeData))
    f.close()
    print(num)





if __name__ == '__main__':
    if len(sys.argv) == 4:
        src = sys.argv[1] # data buffer
        complexity = int(sys.argv[2])
        dataSize = int(sys.argv[3])
        # print(src)
        # print(complexity)
        # print(dataSize)
        genDataset(src, complexity, dataSize)
    else:
        print("Usage: fastcdc.py <databuffer>")