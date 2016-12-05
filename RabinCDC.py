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

def string2numeric_hash(text):
    import hashlib
    return int(hashlib.md5(text).hexdigest()[:8], 16)

def RabinCDC(LBAlist):
    P = 53424
    a = 10
    rolling_hash = 0
    n = len(LBAlist)
    breakindices = []
    for i in range(0,n-1):
        rolling_hash += int(LBAlist[i])
        secondhash = string2numeric_hash(str(rolling_hash))
        if secondhash%9 == 0:
            breakindices.append(i)
            rolling_hash = 0
    return breakindices

def main(src):
    '''
    Input: data buffer src
    '''
    target = open(src, 'r')

    LBAlist = target.readlines()

    breakindices = RabinCDC(LBAlist)

    outputfile = open("RabinOut", 'w')
    for i in breakindices:
        outputfile.write("%s\n" % i)
    outputfile.close()


if __name__ == '__main__':
    if len(sys.argv) == 2:
        src = sys.argv[1] # data buffer
        main(src)
    else:
        print "Usage: fastcdc.py <databuffer>"