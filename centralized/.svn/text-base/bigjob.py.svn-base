#!/usr/bin/env python

"""Module big_job.

This Module is used to launch jobs via the advert service. 

It assumes that an bigjob_agent.py is available on the remote machine.
bigjob_agent.py will poll the advert service for new jobs and run these jobs on the respective
machine .

Background: This approach avoids queueing delays since the advert-launcher.py must be just started via saga.job or saga.cpr
once. All shortrunning task will be started using the protocol implemented by advert_job() and advert_launcher.py
"""

import sys
import getopt
import saga
import time
import uuid
import pdb
import socket
import os
import traceback
import logging

""" Config parameters (will move to config file in future) """
APPLICATION_NAME="BigJob/BigJob"

class bigjob():
    
    def __init__(self, database_host):        
        self.database_host = database_host
        print "init advert service session at host: " + database_host
        self.uuid = uuid.uuid1()
        self.app_url = saga.url("advert://" + database_host + "/"+APPLICATION_NAME + "-" + str(self.uuid) + "/")
        self.app_dir = saga.advert.directory(self.app_url, saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
 	self.state=saga.job.Unknown
        self.pilot_url=""
        print "created advert directory for application: " + self.app_url.get_string()
    
    def start_pilot_job(self, 
                 lrms_url, 
                 bigjob_agent_executable,
                 number_nodes,
                 queue,
                 project,
                 working_directory,
                 userproxy,
                 walltime):
        """ start advert_launcher on specified host """
        if userproxy != None and userproxy != '':
            os.environ["X509_USER_PROXY"]=userproxy
            print "use proxy: " + userproxy
        else:
            print "use standard proxy"

        #register advert entry
        lrms_saga_url = saga.url(lrms_url)
        self.pilot_url = self.app_url.get_string() + "/" + lrms_saga_url.host
        print "create advert entry: " + self.pilot_url
        self.pilot_dir = saga.advert.directory(saga.url(self.pilot_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        # application level state since globus adaptor does not support state detail
        self.pilot_dir.set_attribute("state", str(saga.job.Unknown)) 
        logging.debug("set pilot state to: " + self.pilot_dir.get_attribute("state"))
        self.number_nodes=int(number_nodes)        
 
        # create job description
        jd = saga.job.description()
        jd.number_of_processes = str(number_nodes)
        jd.spmd_variation = "single"
        jd.arguments = [bigjob_agent_executable, self.database_host, self.pilot_url]
        jd.executable = "/bin/bash"
        #jd.executable = bigjob_agent_executable
        if queue != None:
            jd.queue = queue
        if project !=None:
            jd.job_project = [project]
        if walltime!=None:
            jd.wall_time_limit=str(walltime)

        if working_directory != None:
            jd.working_directory = working_directory
        else:
            jd.working_directory = "$(HOME)"
            
            
      #  if not os.path.isdir(jd.working_directory):
       #     os.mkdir(jd.working_directory)
            
        print "Working directory: " + jd.working_directory
        
        jd.output = "stdout-bigjob_agent-" + str(self.uuid) + ".txt"
        jd.error = "stderr-bigjob_agent-" + str(self.uuid) + ".txt"
           
        # Submit jbo
        js = saga.job.service(lrms_saga_url)
        self.job = js.create_job(jd)
        print "Submit pilot job to: " + str(lrms_saga_url)
        self.job.run()
        return self.job
     
    def get_state(self):        
        """ duck typing for get_state of saga.cpr.job and saga.job.job  """
        try:
            return self.job.get_state()
        except:
            return None
    
    def get_state_detail(self): 
        try:
            return self.pilot_dir.get_attribute("state")
        except:
            return None
    
    def get_free_nodes(self):
        jobs = self.pilot_dir.list()
        number_used_nodes=0
        for i in jobs:
            job_dir=None
            try:
                job_dir = self.pilot_dir.open_dir(i.get_string(), saga.advert.Create | saga.advert.ReadWrite) 
            except:
                pass
            if job_dir != None and job_dir.attribute_exists("state") == True\
                and str(job_dir.get_attribute("state"))==str(saga.job.Running):
                job_np = "1"
                if (job_dir.attribute_exists("NumberOfProcesses") == True):
                    job_np = job_dir.get_attribute("NumberOfProcesses")

                number_used_nodes=number_used_nodes + int(job_np)
        return (self.number_nodes - number_used_nodes)

    def cancel(self):        
        """ duck typing for cancel of saga.cpr.job and saga.job.job  """
        print "Cancel Glidin Job"
        try:
            self.job.cancel()
            #self.app_dir.change_dir("..")
            print "delete job: " + str(self.app_url)
            self.app_dir.remove(self.app_url, saga.name_space.Recursive)    
        except:
            pass

    def __repr__(self):
        return self.pilot_url 

    def __del__(self):
        self.cancel()

                    
                    
class subjob():
    
    def __init__(self, database_host):
        """Constructor"""
        self.database_host = database_host
        self.job_url=None
        self.uuid = uuid.uuid1()
        self.job_url = None
        
    def get_job_url(self, pilot_url):
        self.saga_pilot_url = saga.url(pilot_url)
        if(self.saga_pilot_url.scheme=="advert"): #
            pass

        else: # any other url, try to guess pilot job url
            host=""
            try:
                host = self.saga_pilot_url.host
            except:
                pass
            if host =="":
                host=socket.gethostname()
            # create dir for destination url
            self.saga_pilot_url = saga.url("advert://" +  self.database_host + "/"+APPLICATION_NAME + "/" + host)

        # create dir for job
        self.job_url = self.saga_pilot_url.get_string() + "/" + str(self.uuid)
        return self.job_url
#rid is used to identify each of the sub-jobs, and is useful when monitoring the jobs, reading the energies of different replicas
    def submit_job(self, pilot_url, jd, rid):
        """ submit job via advert service to NAMD-Launcher 
            dest_url - url reference to advert job or host on which the advert job is going to run"""
        print "submit job: " + str(pilot_url)
        if self.job_url==None:
            self.job_url=self.get_job_url(pilot_url)

        for i in range(0,3):
            try:
                print "create job entry "
                self.job_dir = saga.advert.directory(saga.url(self.job_url), 
                                             saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
                print "initialized advert directory for job: " + self.job_url
                # put job description attributes to advert
                attributes = jd.list_attributes()                
                for i in attributes:          
                        if jd.attribute_is_vector(i):
                            self.job_dir.set_vector_attribute(i, jd.get_vector_attribute(i))
                        else:
                            logging.debug("Add attribute: " + str(i) + " Value: " + jd.get_attribute(i))
                            self.job_dir.set_attribute(i, jd.get_attribute(i))

                self.job_dir.set_attribute("state", str(saga.job.Unknown))
		self.job_dir.set_attribute("energy", "unknown energy")
                self.job_dir.set_attribute("temp", "unknown temp")
		self.job_dir.set_attribute("replica_id", rid)
                # return self object for get_state() query    
                #logging.debug("Submission time (time to create advert entries): " + str(time.time()-start) + " s")
                return self    
            except:
                traceback.print_exc(file=sys.stdout)
                #time.sleep(2)
                #raise Exception("Unable to submit job")      

    def get_state(self):        
        """ duck typing for get_state of saga.cpr.job and saga.job.job  """
        return self.job_dir.get_attribute("state")

    def get_energy(self):
        """ duck typing for get_state of saga.cpr.job and saga.job.job  """
        return self.job_dir.get_attribute("energy")

    def get_temp(self):
        """ duck typing for get_state of saga.cpr.job and saga.job.job  """
        return self.job_dir.get_attribute("temp")

    def delete_job(self):
        print "delete job and close dirs: " + self.job_url
        try:
            self.job_dir.change_dir("..")
            self.job_dir.remove(saga.url(self.job_url), saga.name_space.Recursive)
            self.job_dir.close()
        except:
            pass

    def __del__(self):
        self.delete_job()
    
    def __repr__(self):        
        if(self.job_url==None):
            return "None"
        else:
            return self.job_url

