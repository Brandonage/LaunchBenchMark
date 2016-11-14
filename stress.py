
import random
import time
import subprocess

loadTypes = ['-P','-p','-B','-d','--sock','--all','-D']
##-P poll: Stress cpu sys and creates system context switch
##-p pipe: stress cpu sys
##-B bigheap: creates paging in and out by overflowing the memory
##-d hdd: writes at disk
##--sock: creates a lot of system context switch
##--all: All the workers. Just chaos
##-D dentry: writes to disk

while True:
    type = loadTypes[random.randrange(0,7,1)]
    if (type=='--all'):
        nWorkers=1
    else:
        nWorkers = random.randrange(3,4,1)
    duration = random.randrange(5,15,1)
    sleepTime = random.randrange(20,60,2)
    print('p = subprocess.Popen([/opt/stress-ng-0.03.11/stress-ng",type,' + str(nWorkers)+',--timeout,' + str(duration)+ ',--quiet])')
    time.sleep(sleepTime)