#!/usr/bin/env python
#  async_qb_buggy#  
#
#  Created by athota1 on 18/08/10.
#  Copyright (c) 2010 __MyCompanyName__. All rights reserved.
#
import saga
import os
import subprocess
import logging
import sys
import time

#Configure here:
HOST = "qb1.loni.org"
REMOTE1 = "gatekeeper.ranger.tacc.teragrid.org"
REMOTE2 = "qb1.loni.org"
REMOTE3 = "painter1.loni.org"
#dirs for replicas
WORK_DIR = "/work/athota1/new_bigjob/decentralized/"
WORK_DIR1= "/work/01297/athota1/new_bigjob/decentralized/"
RPB = 4 #NUMBER_REPLICAS/BIGJOB

def copy_with_saga(i):
   # print "####################start time(npt.conf copy)" + time.asctime(time.localtime(time.time())) + "##################"
    start = time.time()
    if i<RPB:
      os.system("cp "+ WORK_DIR +"agent/"+ str(replica_id)+ "/NPT.conf " + WORK_DIR + "agent/" + str(i) + "/NPT.conf")
     # source_url = saga.url('file://' + WORK_DIR + 'NPT.conf')
     # dest_url = saga.url('file://' + WORK_DIR + 'agent/' + str(i) + '/')
    elif (i>=RPB and i<(2*RPB)):
      source_url = saga.url('file://' + WORK_DIR + 'NPT.conf')
      dest_url = saga.url('gridftp://' +"gridftp.ranger.tacc.teragrid.org:2811" + WORK_DIR1+'agent/'+str(i)+'/')
      sagafile = saga.filesystem.file(source_url)
      try:
        sagafile.copy(dest_url)
      except saga.exception, e:
        print "\n(ERROR) remote ###NPT.CONF####file copy from %s to %s failed"%(HOST, REMOTE1)
    elif (i>=(2*RPB) and i<(3*RPB)):
      source_url = saga.url('file://' + WORK_DIR + 'NPT.conf')
      dest_url = saga.url('gridftp://' + REMOTE2 + WORK_DIR+'agent/'+str(i)+'/')
      sagafile = saga.filesystem.file(source_url)
      try:
        sagafile.copy(dest_url)
      except saga.exception, e:
        print "\n(ERROR) remote ###NPT.CONF####file copy from %s to %s failed"%(HOST, REMOTE1)
    else:
      source_url = saga.url('file://' + WORK_DIR + 'NPT.conf')
      dest_url = saga.url('gridftp://' + REMOTE3 + WORK_DIR+'agent/'+str(i)+'/')
      sagafile = saga.filesystem.file(source_url)
      try:
        sagafile.copy(dest_url)
      except saga.exception, e:
        print "\n(ERROR) remote ###NPT.CONF####file copy from %s to %s failed"%(HOST, REMOTE1)
    #  print str(i)
   # print "####################end time(npt.conf copy)" + time.asctime(time.localtime(time.time())) + "##################"
    #print "time to copy: " + str(time.time() - start)
    return None

def NAMD_config(some_temp):
   #print "#################### namd config prep start time" + time.asctime(time.localtime(time.time())) + "##################"
# config prep when re-launching replicas   
   start = time.time()
   ifile = open("NPT.conf")   # should be changed if a different name is going to be used
   lines = ifile.readlines()
   for line in lines:
      if line.find("desired_temp") >= 0 and line.find("set") >= 0:
         lines[lines.index(line)] = "set desired_temp %s \n"%(some_temp)        
   ifile.close()
   ofile = open("NPT.conf", "w")
   for line in lines:
     ofile.write(line)
   ofile.close()
  # print "####################end time config prep" + time.asctime(time.localtime(time.time())) + "##################"
   #print "time to prep: "+ str(time.time() - start)


def read_temp(replica_id):
    enfile = open("stdout-" + str(replica_id))
    lines = enfile.readlines()
    for line in lines:
      items = line.split()
      if len(items) > 0:
        if items[0] in ("ENERGY:"):
           en = items[12]
   # print "(DEBUG) energy : " + str(en)logging.debug
    return en

if __name__ == "__main__":
   
    replica_id = sys.argv[2]
    LOG_FILENAME = 'logging.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
    logging.debug('parsing')
    logging.debug("REPLICA # " + str(replica_id))
    nodelist = sys.argv[4]
    logging.debug("the nodes assigned =" + str(nodelist))
    tot_reps = sys.argv[3]
    logging.debug("total number of replicas " + str(tot_reps))
   # command = "mpirun" + " -np " + "16" + " -machinefile " + nodelist + " " +WORK_DIR+ "agent/" +str(replica_id) + "/namd2" + " "+ WORK_DIR+ "agent/"+str(replica_id)+"/NPT.conf"
    if int(replica_id)<RPB: 
      command = "mpirun" + " -np " + "16" + " -machinefile " + nodelist + " " +WORK_DIR+ "namd2" + " "+"NPT.conf"
    else: 
      command = "mpirun" + " -np " + "16" + " -machinefile " + nodelist + " " +WORK_DIR1+"namd2" + " "+"NPT.conf"
    logging.debug("command is :" + str(command))
    stdout = open("stdout-"+str(replica_id), "w")
    stderr = open("stderr-"+str(replica_id), "w")    
    agent_url = saga.url("advert://fortytwo.cct.lsu.edu/"+"BigJob/BigJob" + "-" + sys.argv[1] + "/"+str(replica_id)+"/")
    logging.debug("the replica's advert url is:" + agent_url.get_string())
    agent_dir = saga.advert.directory(agent_url, saga.advert.Create | saga.advert.ReadWrite)        
    p = subprocess.Popen(args=command, executable="/bin/bash", stderr=stderr,stdout=stdout,cwd=os.getcwd(),shell=True)
    agent_dir.set_attribute("state", str("Running"))
    logging.debug("job started running, for the first time, now monitoring the job")
    logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
    while 1:
      p_state = p.poll()   
      logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################") 
      if (p_state != None and (p_state==0 or p_state==255)):
        logging.debug(" finished running, 1st time ")
        local_temp = str(read_temp(replica_id))        
        agent_dir.set_attribute("temp", local_temp)
        logging.debug("temp is " + str(local_temp))
        agent_dir.set_attribute("state", str("Free"))
        logging.debug("now the replica is in Free state")
        break
      else:
        time.sleep(2)
    while 1:    
     logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
     if (str(agent_dir.get_attribute("state"))=="Ready"):
       logging.debug("state is pending/ready, set by another replica")              
       p = subprocess.Popen(args=command, executable="/bin/bash", stderr=stderr,stdout=stdout,cwd=os.getcwd(),shell=True)
       agent_dir.set_attribute("state", str("Running"))
       logging.debug("subprocess started, command")
       logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
       while 1:
          p_state = p.poll()  
          logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")      
          if (p_state != None and (p_state==0 or p_state==255)):
            logging.debug("monitoring state, now done ")                 
            local_temp = str(read_temp(replica_id))        
            logging.debug("replica temp is :"+ str(local_temp) + ", now in free state")
            agent_dir.set_attribute("temp", local_temp)               
            agent_dir.set_attribute("state", str("Free"))
            break
          else:
            time.sleep(2)
     elif (str(agent_dir.get_attribute("state"))== "Free"):
        logging.debug("replica is in free state, case II, checking for another rep in free state")
        logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
        for i in range(0, int(tot_reps)):
         if i!=int(replica_id): 
          logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
          temp_url=saga.url("advert://fortytwo.cct.lsu.edu/"+"BigJob/BigJob" + "-" + sys.argv[1] + "/"+str(i)+"/") 
          logging.debug("looking in this replica url,:" + temp_url.get_string())
          temp_dir = saga.advert.directory(temp_url, saga.advert.Create | saga.advert.ReadWrite)  
          logging.debug("the sttae is: " + str(temp_dir.get_attribute("state")))
          if str(temp_dir.get_attribute("state"))=="Free" and str(agent_dir.get_attribute("state"))== "Free":    
            logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")     
            logging.debug("found another replica in free state, both replicas in free state, start making exchange")
            temp_dir.set_attribute("state", str("Pending"))
            agent_dir.set_attribute("state", str("Pending"))           
            local_temp = agent_dir.get_attribute("temp")
            remote_temp = temp_dir.get_attribute("temp")   
            logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")         
            NAMD_config(str(local_temp))            
            copy_with_saga(i)
            logging.debug("local replica temp is: " + str(local_temp) +", assigned to remote replica")
            NAMD_config(str(remote_temp))
           # copy_with_saga(str(replica_id))
            logging.debug("remote replica temp is: " + str(remote_temp) + ", given to local replica")
            logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
            agent_dir.set_attribute("state", str("Running"))
            temp_dir.set_attribute("state", str("Ready"))
            p = subprocess.Popen(args=command, executable="/bin/bash", stderr=stderr,stdout=stdout,cwd=os.getcwd(),shell=True) 
            logging.debug("restarted replica, states of both replicas set to Running")
            logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")            
            count_url = saga.url("advert://fortytwo.cct.lsu.edu/"+"BigJob/BigJob" + "-" + sys.argv[1] + "/Count")
            logging.debug("monitoring and setting the count at: " + count_url.get_string())
            count_dir = saga.advert.directory(count_url, saga.advert.Create | saga.advert.ReadWrite)
            count = int(count_dir.get_attribute("count"))
            logging.debug("initial count is :" + str(count))
            count = count+1
            count_dir.set_attribute("count", str(count))
            logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
            logging.debug("count after exchange is:" + str(count))
            while 1:
              p_state = p.poll()      
              logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")   
              if (p_state != None and (p_state==0 or p_state==255)):
                 logging.debug("monitoring the restarted rplica, now done ")
                 local_temp = str(read_temp(replica_id))
                 logging.debug("new temp is : " + str(local_temp))
                 agent_dir.set_attribute("temp", local_temp)
                 agent_dir.set_attribute("state", str("Free"))
                 logging.debug("state now free")
                 break
              else:
                 time.sleep(2) 
            logging.debug("getting out of the for loop")
            break          
          elif str(temp_dir.get_attribute("state"))!="Free": #temp replica's state changed from free to pending, look for other replicas
            logging.debug("temp replica is not free anymore, look for other replicas")
            logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
            pass
          else: #local replica's state changed from free to pending
            logging.debug("initially both were free, but now the local replica is in pending, breaking the loop and going into while loop")
            logging.debug("####################" + time.asctime(time.localtime(time.time())) + "##################")
            logging.debug("the states are, remote and local respectively :" +  str(temp_dir.get_attribute("state")) + "and " +str(agent_dir.get_attribute("state")))
            break
         else: #pass if it's the same replica
          logging.debug(" same replica####################" + time.asctime(time.localtime(time.time())) + "##################")
          pass   
