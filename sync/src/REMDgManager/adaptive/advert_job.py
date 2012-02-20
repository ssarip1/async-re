#!/usr/bin/env python

"""Module advert_job.

This Module is used to launch jobs via the advert service. 

It assumes that an advert-launcher.py agent is  running on the remote machine.
advert-launcher.py will poll the advert service for new jobs and run these jobs on the respective
machine  

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
CPR = False
        
class advert_glidin_job():
    
    def __init__(self, database_host):        
        self.database_host = database_host
        print "init advert service session at host: " + database_host
        self.uuid = uuid.uuid1()
        self.app_url = saga.url("advert://" + database_host + "/"+APPLICATION_NAME + "-" + str(self.uuid) + "/")
        self.app_dir = saga.advert.directory(self.app_url, saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        print "created advert directory for application: " + self.app_url.get_string()
    
    def start_glidin_job(self, 
                 lrms_url, 
                 replica_agent_executable,
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
        # SAGA Context is broken at this point
        #s = saga.session()
        #ctx = saga.context("globus")
        #ctx.set_attribute ("UserProxy", userproxy); 
        #ctx.set_defaults (); 
        #s.add_context(ctx)

        #register advert entry
        lrms_saga_url = saga.url(lrms_url)
        self.glidin_url = self.app_url.get_string() + "/" + lrms_saga_url.host
        print "create advert entry: " + self.glidin_url
        self.glidin_dir = saga.advert.directory(saga.url(self.glidin_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        # application level state since globus adaptor does not support state detail
        self.glidin_dir.set_attribute("state", str(saga.job.Unknown)) 
        logging.debug("set glidin state to: " + self.glidin_dir.get_attribute("state"))
        if CPR==True:
                jd = saga.cpr.description()
        else:    
                jd = saga.job.description()

        jd.number_of_processes = str(number_nodes)
        jd.spmd_variation = "single"
        jd.arguments = [self.database_host, self.glidin_url]
        if (replica_agent_executable == None or replica_agent_executable == ""):
            jd.executable = "$(HOME)/src/REMDgManager/adaptive/advert_launcher.sh" # backward compatibility to be removed
        else:
            jd.executable = replica_agent_executable
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
        
        jd.output = "stdout-advert-launcher-" + str(self.uuid) + ".txt"
        jd.error = "stderr-advert-launcher-" + str(self.uuid) + ".txt"
           
        if CPR==True: 
            js = saga.cpr.service(lrms_saga_url)
            self.job = js.create_job(jd, jd)
            print "Submit CPR Glide-In job to: " + str(lrms_saga_url)
            self.job.run()
        else:
            js = saga.job.service(lrms_saga_url)
            self.job = js.create_job(jd)
            print "Submit Non-CPR Glide-In job to: " + str(lrms_saga_url)
            self.job.run()
        return self.job
     
    def get_state(self):        
        """ duck typing for get_state of saga.cpr.job and saga.job.job  """
        return self.job.get_state()
    
    def get_state_detail(self): 
        return self.glidin_dir.get_attribute("state")
    
    def cancel(self):        
        """ duck typing for cancel of saga.cpr.job and saga.job.job  """
        print "Cancel Glidin Job"
        self.job.cancel()
        try:
            #self.app_dir.change_dir("..")
            print "delete job: " + str(self.app_url)
            self.app_dir.remove(self.app_url, saga.name_space.Recursive)    
        except:
            pass

    def __repr__(self):
        return self.glidin_url 

    def __del__(self):
        self.cancel()

                    
                    
class advert_job():
    
    def __init__(self, database_host):
        """Constructor"""
        self.database_host = database_host
        self.job_url=None
        self.uuid = uuid.uuid1()
        self.job_url = None
        
    def get_job_url(self, glidin_url):
        self.saga_glidin_url = saga.url(glidin_url)
        if(self.saga_glidin_url.scheme=="advert"): #
            pass

        else: # any other url, try to guess glidin job url
            host=""
            try:
                host = self.saga_glidin_url.host
            except:
                pass
            if host =="":
                host=socket.gethostname()
            # create dir for destination url
            self.saga_glidin_url = saga.url("advert://" +  self.database_host + "/"+APPLICATION_NAME + "/" + host)
            #self.glidin_dir = saga.advert.directory(self.saga_glidin_url, 
            #                                        saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        # create dir for job
        self.job_url = self.saga_glidin_url.get_string() + "/" + str(self.uuid)
        return self.job_url

    def submit_job(self, glidin_url, jd):
        """ submit job via advert service to NAMD-Launcher 
            dest_url - url reference to advert job or host on which the advert job is going to run"""
        print "submit job: " + str(glidin_url)
        if self.job_url==None:
            self.job_url=self.get_job_url(glidin_url)

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

