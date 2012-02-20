#!/usr/bin/env python2.3
#
#  async-re.py
#  
#
#  Created by athota1 on 08/04/10.
#  Copyright (c) 2010 __MyCompanyName__. All rights reserved.
#

""" Example application demonstrating job submission via bigjob 
    advert_job implementation of BigJob is used
"""

#If 2 bigjobs, make the number of subjobs a multiple of two, if 3 BJs - a multiple of 3 and so on.

import saga
import os
import bigjob
import time
import pdb

#Configure here:
BIGJOB_SIZE = 512
NUMBER_EXCHANGES = 64
NUMBER_BIGJOBS= 1
NUMBER_REPLICAS = 32
CPR = 16 # cores per replica
HOST = "qb1.loni.org"
REMOTE1 = "louie1.loni.org"
REMOTE2 = "qb1.loni.org"
REMOTE3 = "painter1.loni.org"
advert_host = "fortytwo.cct.lsu.edu"
#dirs for replicas
WORK_DIR = "/work/athota1/new_bigjob/bigjob/"
WALLTIME = "10"
REPLICA_DIR = "/work/athota1/new_bigjob/bigjob/NAMD_files/"
RPB = 32 #NUMBER_REPLICAS/BIGJOB


def stage_files(i):
   start = time.time()
   print "####################" + time.asctime(time.localtime(time.time())) + "##################"
   print "start staging files"
   if i<RPB:
     try:
        os.mkdir(WORK_DIR + 'agent/' + str(i))
     except OSError:
        pass
     os.system("cp -r " + REPLICA_DIR + "* " + WORK_DIR+ "agent/" + str(i)+ "/")
   elif(i>=RPB and i<(2*RPB)):
     try:
        os.mkdir(WORK_DIR + 'agent/' + str(i))
     except OSError:
        pass
     os.system("gsiscp -r " + WORK_DIR + "agent/" + str(i) + " " + REMOTE1 + ":" + WORK_DIR + "agent/" ) 
     os.system("gsiscp -r " + REPLICA_DIR + "* " + REMOTE1 + ":" + WORK_DIR+ "agent/" + str(i)+ "/")
   elif (i>=(2*RPB) and i<(3*RPB)):
     try:
        os.mkdir(WORK_DIR + 'agent/' + str(i))
     except OSError:
        pass
     os.system("gsiscp -r " + WORK_DIR + "agent/" + str(i) + " " + REMOTE2 + ":" + WORK_DIR + "agent/" ) 
     os.system("gsiscp -r " + REPLICA_DIR + "* " + REMOTE2 + ":" + WORK_DIR+ "agent/" + str(i)+ "/")
   else:
     try:
        os.mkdir(WORK_DIR + 'agent/' + str(i))
     except OSError:
        pass
     os.system("gsiscp -r " + WORK_DIR + "agent/" + str(i) + " " + REMOTE3 + ":" + WORK_DIR + "agent/" ) 
     os.system("gsiscp -r " + REPLICA_DIR + "* " + REMOTE3 + ":" + WORK_DIR+ "agent/" + str(i)+ "/")       
   print "####################" + time.asctime(time.localtime(time.time())) + "##################"
   print "end staging files"
   print "time to stage files: " + str(time.time()-start) +" s"
def stage_ifiles(i):
   if not i%2:
     try:
        os.mkdir(WORK_DIR + 'agent/' + str(i))
     except OSError:
        pass
     for ifile in os.listdir(REPLICA_DIR):
        source_url = saga.url('file://' + REPLICA_DIR + ifile)
        dest_url = saga.url('file://' + WORK_DIR + 'agent/'+ str(i)+'/')
        sagafile = saga.filesystem.file(source_url) 
        try: 
           sagafile.copy(dest_url)
        except saga.exception, e:
           print str(e) + "\n(ERROR) local file ####STAGING### copy from %s to %s failed"%(REPLICA_DIR, HOST)
   else:
     try:
        os.mkdir(WORK_DIR + 'agent/' + str(i))
     except OSError:
        pass
     os.system("gsiscp -r " + WORK_DIR + "agent/" + str(i) + " " + REMOTE1 + ":" + WORK_DIR + "agent/" ) 
    # s_url = saga.url('file://' + WORK_DIR + 'agent/' + str(i)+'/')
    # d_url = saga.url('gridftp://' + REMOTE1 + WORK_DIR + 'agent/')
    # sagadir = saga.filesystem.directory(s_url)
    # try: 
    #    sagadir.copy(d_url)
    # except saga.exception, e:
    #    print "\n(ERROR) creating directories on remote machine %s  failed or directory already exists"%(REMOTE1)
     for ifile in os.listdir(REPLICA_DIR):
       source_url = saga.url('file://' + REPLICA_DIR + ifile)
       dest_url = saga.url('gridftp://' + REMOTE1 + WORK_DIR + 'agent/' + str(i)+'/')
       sagafile = saga.filesystem.file(source_url) 
       try: 
         sagafile.copy(dest_url)
       except saga.exception, e:
         print str(e) + "\n(ERROR) remote file ####STAGING### copy from %s to %s failed"%(HOST, REMOTE1)

def copy_with_saga(i):
    print "####################start time(npt.conf copy)" + time.asctime(time.localtime(time.time())) + "##################"
    start = time.time()
    if i<RPB:
      os.system("cp "+ WORK_DIR + "/NPT.conf " + WORK_DIR + "agent/" + str(i) + "/NPT.conf")
     # source_url = saga.url('file://' + WORK_DIR + 'NPT.conf')
     # dest_url = saga.url('file://' + WORK_DIR + 'agent/' + str(i) + '/')
    elif (i>=RPB and i<(2*RPB)):
      source_url = saga.url('file://' + WORK_DIR + 'NPT.conf')
      dest_url = saga.url('gridftp://' + REMOTE1 + WORK_DIR+'agent/'+str(i)+'/')
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
    print "####################end time(npt.conf copy)" + time.asctime(time.localtime(time.time())) + "##################"
    print "time to copy: " + str(time.time() - start)
    return None
             
def prepare_NAMD_config(r, i):
   print "#################### namd config prep start time" + time.asctime(time.localtime(time.time())) + "##################"
# config prep when re-launching replicas   
   start = time.time()
   ifile = open(WORK_DIR+ "NPT.conf")   # should be changed if a different name is going to be used
   lines = ifile.readlines()
   for line in lines:
      if line.find("desired_temp") >= 0 and line.find("set") >= 0:
         lines[lines.index(line)] = "set desired_temp %s \n"%(str(temperature[r]))
         print "new temperatures being set, re-launching#" + str(i) + "whose new temp=" + str(temperature[r])
   ifile.close()
   ofile = open(WORK_DIR+ "NPT.conf", "w")
   for line in lines:
     ofile.write(line)
   ofile.close()
   print "####################end time config prep" + time.asctime(time.localtime(time.time())) + "##################"
   print "time to prep: "+ str(time.time() - start)
def NAMD_config(i):
  print "####################initial prep of config start" + time.asctime(time.localtime(time.time())) + "##################"
#initial prep of config,for the first launch of replicas
  start = time.time()
  ifile = open(WORK_DIR+ "NPT.conf")   # should be changed if a different name is going to be used
  lines = ifile.readlines()
  for line in lines:
     if line.find("desired_temp") >= 0 and line.find("set") >= 0:
      # if(i==0):
        lines[lines.index(line)] = "set desired_temp %s \n"%(str(temps[i]))
        print "initial temperature has been set for replica id " + str(i)+ "with" + str(temps[i])
      # else:
       #   lines[lines.index(line)] = "set desired_temp %s \n"%(str(temps[i]))
  ifile.close()
  ofile = open(WORK_DIR+ "NPT.conf", "w")
  for line in lines:
    ofile.write(line)
  ofile.close()
  print "####################" + time.asctime(time.localtime(time.time())) + "end confg prep##################"
  print "time to initail prep of config: " + str(time.time() - start)

""" Test Job Submission via Advert """
if __name__ == "__main__":
    print str(time.time()) + "= start time######################"
    print "####################" + time.asctime(time.localtime(time.time())) + "bigjob start##################"
    start = time.time()
#range of temperatures
    temps=[]
    t=300
    for i in range(0,NUMBER_REPLICAS):
      temp = t
      t = t+10
      temps.append(temp)

##################################################################################  
  # Start BigJob
    # Parameter for BigJob
    bigjob_agent = os.getcwd() + "/bigjob_agent_launcher.sh" # path to agent
    #bigjob_agent = "/bin/echo"
    nodes = BIGJOB_SIZE # number nodes for agent
    workingdirectory=os.getcwd() +"/agent"  # working directory for agent
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)

    bjs=[]
    i=0
    for i in range(0,NUMBER_BIGJOBS):
      bj = bigjob.bigjob(advert_host)
      bjs.append(bj)
      if(i==0):
        lrms_url = "gram://" + HOST + "/jobmanager-pbs" 
      elif(i==1):
        lrms_url = "gram://" + REMOTE1 + "/jobmanager-pbs"
      elif(i==2):
        lrms_url = "gram://" + REMOTE2 + "/jobmanager-pbs"
      else:
        lrms_url = "gram://" + REMOTE3 + "/jobmanager-pbs"
      bjs[i].start_pilot_job(lrms_url,
                            bigjob_agent,
                            nodes,
                            None,
                            None,
                            workingdirectory,userproxy,20)
      print "Start Pilot Job/BigJob: " + bigjob_agent + " at: " + lrms_url
      print "Pilot Job/BigJob URL: " + bjs[i].pilot_url + " State: " + str(bjs[i].get_state())
      print "####################" + time.asctime(time.localtime(time.time())) + "end bigjob lauch, start config, staging file##################"
      print "time to laucnh bjs: " + str(time.time() - start)
    ##########################################################################################
    # Submit SubJob through BigJob
    i=0
    jds=[]
    sjs=[]
    for i in range(0, NUMBER_REPLICAS):
      stage_files(i)
      jd = saga.job.description()
      jd.executable = "namd2"
      jd.number_of_processes = "16"
      jd.spmd_variation = "mpi"
   # jd.arguments = ["NPT.conf"]
      jd.working_directory = WORK_DIR + "agent/" + str(i)+"/"
      #os.system("cp NPT.conf NPT.conf")
      jd.arguments = ["NPT.conf"]
      jd.output = str(i) + "/stdout-" + str(i) + ".txt"
      jd.error = str(i) + "/stderr-" + str(i) + ".txt"  	
      jds.append(jd)
      sj = bigjob.subjob(advert_host)
      sjs.append(sj)
      #prepare config and scp other files to remote machine
      NAMD_config(i)
      if i<RPB:
        j = 0   
        copy_with_saga(i)
        sjs[i].submit_job(bjs[j].pilot_url, jds[i],str(i))
      elif (i>=RPB and i<(2*RPB)):
        j = 1   
        copy_with_saga(i)
        sjs[i].submit_job(bjs[j].pilot_url, jds[i],str(i))
      elif (i>=(2*RPB) and i<(3*RPB)):
        j = 2   
        copy_with_saga(i)
        sjs[i].submit_job(bjs[j].pilot_url, jds[i],str(i))
      else: 
        j = 3
        #os.system("gsiscp NPT-" + str(i) + ".conf %s:%s"%(REMOTE1, WORK_DIR))
        copy_with_saga(i)
        sjs[i].submit_job(bjs[j].pilot_url, jds[i],str(i))
    print "####################" + time.asctime(time.localtime(time.time())) + "end prep of sub jobs##################"    
    count=0
    while (count < NUMBER_EXCHANGES):
      print "exchange count=" + str(count)
#################################################################################            
      i = 0
      state=[]
      energy=[]
      temperature=[]
      print "####################" + time.asctime(time.localtime(time.time())) + "start get attributes##################"
      for i in range(0,NUMBER_REPLICAS):
       states = str(sjs[i].get_state())
       energies = str(sjs[i].get_energy())
       temperatures = str(sjs[i].get_temp())
       state.append(states)
       energy.append(energies)
       temperature.append(temperatures)
       print "current state= " + str(state[i]) + " where: replica# is" +str(i) + ", current energy: " + str(energy[i])+ "current temp " + str(temperature[i])
      # time.sleep(1)
      print "####################" + time.asctime(time.localtime(time.time())) + "end get attributes##################"
#################################################################################             
      for i in range(0, NUMBER_REPLICAS):
        if(state[i]=="Done"):
          j=i #exclude the replica itself when looking for partners
#############################################      
          f=0
          list=[]
          for f in range(0, NUMBER_REPLICAS):
            print "found a replica in Done state, looking for other replicas in Done state"
            print time.asctime(time.localtime(time.time()))+ " ######## searching for replica for exchange"
            if((state[f]=="Done") and (f!=j)):
              list.append(f)
              print str(f) + "-- replica is in Done state"
            elif(f==j): 
              print "checking the same replica"
            else:
              print str(f) + "not in Done state"
#################################################################################              
          if len(list)!=0:
            print "possible replicas for exchange found"
            k = 0
            for k in list:
              if (float(energy[k]) < 1):
                print time.asctime(time.localtime(time.time())) + " ######## replica selected for exchange"
                print "replica chosen for exchange is" + str(k)
                print "replica for which selection was made" + str(i)
                print "assigning the new temepratures and re-starting the replicas"
                prepare_NAMD_config(k, i) 
                if i<RPB:
                  j=0
                  copy_with_saga(i)
                  sjs[i].submit_job(bjs[j].pilot_url, jds[i], str(i))
                elif (i>=RPB and i<(2*RPB)):
                  j=1                  
                  #os.system("gsiscp NPT-" + str(i) + ".conf %s:%s"%(REMOTE1, WORK_DIR))  
                  copy_with_saga(i)
                  sjs[i].submit_job(bjs[j].pilot_url, jds[i], str(i))
                elif (i>=(2*RPB) and i<(3*RPB)):
                  j=2                  
                  #os.system("gsiscp NPT-" + str(i) + ".conf %s:%s"%(REMOTE1, WORK_DIR))  
                  copy_with_saga(i)
                  sjs[i].submit_job(bjs[j].pilot_url, jds[i], str(i))
                else:
                  j=3                  
                  #os.system("gsiscp NPT-" + str(i) + ".conf %s:%s"%(REMOTE1, WORK_DIR))  
                  copy_with_saga(i)
                  sjs[i].submit_job(bjs[j].pilot_url, jds[i], str(i))                
                prepare_NAMD_config(i, k)
                if k<RPB:
                  j=0
                  copy_with_saga(k)
                  sjs[k].submit_job(bjs[j].pilot_url, jds[k], str(k))
                elif (k>=RPB and k<(2*RPB)):
                  j=1
                  #os.system("gsiscp NPT-" + str(k) + ".conf %s:%s"%(REMOTE1, WORK_DIR))
                  copy_with_saga(k)
                  sjs[k].submit_job(bjs[j].pilot_url, jds[k], str(k))
                elif (k>=(2*RPB) and k<(3*RPB)):
                  j=2
                  #os.system("gsiscp NPT-" + str(k) + ".conf %s:%s"%(REMOTE1, WORK_DIR))
                  copy_with_saga(k)
                  sjs[k].submit_job(bjs[j].pilot_url, jds[k], str(k))
                else:
                  j=3
                  #os.system("gsiscp NPT-" + str(k) + ".conf %s:%s"%(REMOTE1, WORK_DIR))
                  copy_with_saga(k)
                  sjs[k].submit_job(bjs[j].pilot_url, jds[k], str(k))
                count = count + 1
                print time.asctime(time.localtime(time.time()))+ " ######## exchange completed"
                break
              else:
                print str(len(list))+ " = length of list, compared replica not selected, comparing other replicas"
            break    
        else: 
          pass  
            
#################################################################################          
          
      print "count=" + str(count)
    print str(time.time()) + "= end time######################"
   # Cleanup - stop BigJob
    for i in range(0, NUMBER_BIGJOBS):
     bjs[i].cancel()


