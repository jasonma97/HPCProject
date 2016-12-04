'''
This is a python implementation of Fixed Window Chunking.
Author: Kanishk Tantia, Jason Ma
Date:  December 3rd, 2016
'''
import sys

def main(src):
   
    '''
    Input: data buffer src
    '''
    target = open(src, 'r')
    LBAlist = len(target.readlines())

    n = 8

    breakindices = []

    for i in range(1,LBAlist):
    	if i%n == 0:
    		breakindices.append(i)
    	elif i == LBAlist:
    		breakindices.append(i)

    outputfile = open("FixedOutput", 'w')
    for i in breakindices:
        outputfile.write("%s\n" % i)

    outputfile.close()



if __name__ == '__main__':
    if len(sys.argv) == 2:
        src = sys.argv[1] # data buffer
        main(src)
    else:
        print "Usage: fixedc.py <databuffer>"