#!/home/luckow/sw/python-2.5.2/bin/python

import sys
import os
import saga
import subprocess
import socket
import threading
import time
import pdb
import traceback
import signal
import ConfigParser

CONFIG_FILE="bigjob_agent.conf"
class bigjob_agent:
    
    """BigJob Agent:
       - reads new job information from advert service
       - starts new jobs
       - monitors running jobs """
   
    """Constructor"""
    def __init__(self, args):
        
        self.database_host = args[1]
        # objects to store running jobs and processes
        self.jobs = []
        self.processes = {}
        self.freenodes = []
        self.busynodes = []
        self.restarted = {}

        # read config file
        conf_file = os.path.dirname(args[0]) + "/" + CONFIG_FILE
        config = ConfigParser.ConfigParser()
        print ("read configfile: " + conf_file)
        config.read(conf_file)
        default_dict = config.defaults()
        self.CPR = default_dict["cpr"]
        self.SHELL=default_dict["shell"]
        self.MPIRUN=default_dict["mpirun"]
        print "cpr: " + self.CPR + " mpi: " + self.MPIRUN + " shell: " + self.SHELL
        
        # init cpr monitoring
        self.init_cpr()
        # init rms (SGE/PBS)
        self.init_rms()

        self.failed_polls = 0
        # open advert service base url
        hostname = socket.gethostname()
        self.base_url = args[2]
        print "Open advert: " + self.base_url
        try:
            self.base_dir = saga.advert.directory(saga.url(self.base_url), saga.advert.Create | saga.advert.ReadWrite)
        except:
            print "No advert entry found at specified url: " + self.base_url

        # update state of glidin job to running
        self.update_glidin_state()
        # start background thread for polling new jobs and monitoring current jobs
        self.launcher_thread=threading.Thread(target=self.start_background_thread())
        self.launcher_thread.start()
        
    def update_glidin_state(self):     
        print "update state of glidin job to: " + str(saga.job.Running)
        return self.base_dir.set_attribute("state", str(saga.job.Running))
    
    def init_rms(self):
        if(os.environ.get("PBS_NODEFILE")!=None):
            return self.init_pbs()
        elif(os.environ.get("PE_HOSTFILE")!=None):
            return self.init_sge()
        else:
            return self.init_local()
        return None

    def init_local(self):
        """ initialize free nodes list with dummy (for fork jobs)"""
        self.freenodes.append("localhost")

    def init_sge(self):
        """ initialize free nodes list from SGE environment """
        sge_node_file = os.environ.get("PE_HOSTFILE")    
        if sge_node_file == None:
                return
        f = open(sge_node_file)
        sgenodes = f.readlines()
        f.close()
        for i in sgenodes:    
        
            columns = i.split()                
            try:
                for j in range(0, int(columns[1])):
                    print "add host: " + columns[0]
                    self.freenodes.append(columns[0]+"\n")
            except:
                    pass
        return self.freenodes            

    def init_pbs(self):
        """ initialize free nodes list from PBS environment """
        pbs_node_file = os.environ.get("PBS_NODEFILE")    
        if pbs_node_file == None:
            return
        f = open(pbs_node_file)
        self.freenodes = f.readlines()
        f.close()

        # check whether pbs node file contains the correct number of nodes
        num_cpus = self.get_num_cpus()
        node_dict={}
        for i in set(self.freenodes):
           node_dict[i] = self.freenodes.count(i)
           if node_dict[i] < num_cpus:
                node_dict[i] = num_cpus
    
        self.freenodes=[]
        for i in node_dict.keys():
            print "host: " + i + " nodes: " + str(node_dict[i])
            for j in range(0, node_dict[i]):
                print "add host: " + i
                self.freenodes.append(i)

    def get_num_cpus(self):
        cpuinfo = open("/proc/cpuinfo", "r")
        cpus = cpuinfo.readlines()
        cpuinfo.close()
        num = 0
        for i in cpus:
                if i.startswith("processor"):
                        num = num+1
        return num
        
        
    def print_attributes(self, advert_directory):
        """ for debugging purposes 
        print attributes of advert directory """
        
        print "Job: "+advert_directory.get_url().get_string()+ " State: " + advert_directory.get_attribute("state")

        #attributes = advert_directory.list_attributes()                
        #for i in attributes:
        #    if (advert_directory.attribute_is_vector(i)==False):
        #        print "attribute: " + str(i) +  " value: " + advert_directory.get_attribute(i)
        #    else:
        #        print "attribute: " + str(i)
        #        vector = advert_directory.get_vector_attribute(i) 
        #        for j in vector:
        #            print j
     
    def execute_job(self, job_dir):
        """ obtain job attributes from advert and execute process """
        state=None
        try:
            state = job_dir.get_attribute("state")
        except:
            print "Could not access job state... skip execution attempt"
        if(state==str(saga.job.Unknown) or
            state==str(saga.job.New)):
            try: 
                job_dir.set_attribute("state", str(saga.job.New))
                self.print_attributes(job_dir)        
                numberofprocesses = "1"
                if (job_dir.attribute_exists("NumberOfProcesses") == True):
                    numberofprocesses = job_dir.get_attribute("NumberOfProcesses")
                
                spmdvariation="single"
                if (job_dir.attribute_exists("SPMDVariation") == True):
                    spmdvariation = job_dir.get_attribute("SPMDVariation")
                
                arguments = ""
                if (job_dir.attribute_exists("Arguments") == True):
                    for i in job_dir.get_vector_attribute("Arguments"):
                        arguments = arguments + " " + i
                        print str(i) + "###### arguments ######"
                executable = job_dir.get_attribute("Executable")
                
                workingdirectory = os.getcwd() 
                if (job_dir.attribute_exists("WorkingDirectory") == True):
                        workingdirectory =  job_dir.get_attribute("WorkingDirectory")
                
                output="stdout"
                if (job_dir.attribute_exists("Output") == True):
                        output = job_dir.get_attribute("Output")
                        
                error="stderr"
                if (job_dir.attribute_exists("Error") == True):
                       error = job_dir.get_attribute("Error")
               
                # append job to job list
                self.jobs.append(job_dir)
                
                # create stdout/stderr file descriptors
                output_file = os.path.abspath(output)
                error_file = os.path.abspath(error)
                print "stdout: " + output_file + " stderr: " + error_file
                stdout = open(output_file, "w")
                stderr = open(error_file, "w")
               # command = executable + " " + arguments
                
                # special setup for MPI NAMD jobs
                machinefile = self.allocate_nodes(job_dir)
                command = executable + " " + arguments + " " + machinefile
                host = "localhost"
                try:
                    machine_file_handler = open(machinefile, "r")
                   # os.system("cp "+ machinefile + " /work/athota1/machinefile")
                    node= machine_file_handler.readlines()
                    machine_file_handler.close()
                    host = node[0].strip()
                   # print str(node) + "abhinav"
                   # os.system("export PBS_NODEFILE="+machinefile)
                except:
                    pass
                # start application process
                if (spmdvariation.lower( )=="mpi"):
                     if(machinefile==None):
                         print "Not enough resources to run: " + job_dir.get_url().get_string() 
                         return # job cannot be run at the moment
                     command = self.MPIRUN + " -np " + numberofprocesses + " -machinefile " + machinefile + " " + command
                     #if (host != socket.gethostname()):
                     #    command ="ssh  " + host + " \"cd " + workingdirectory + "; " + command +"\""     
                else:
                    command ="ssh  " + host + " \"cd " + workingdirectory + "; " + command +"\""     
                shell = self.SHELL 
                print "execute: " + command + " in " + workingdirectory + " from: " + str(socket.gethostname()) + " (Shell: " + shell +")"
                # bash works fine for launching on QB but fails for Abe :-(
                
                p = subprocess.Popen(args=command, executable=shell, stderr=stderr,stdout=stdout,cwd=workingdirectory,shell=True)
                print "started " + command
                self.processes[job_dir] = p
                job_dir.set_attribute("state", str(saga.job.Running))
            except:
                traceback.print_exc(file=sys.stdout)
            
    def allocate_nodes(self, job_dir):
        """ allocate nodes
            allocated nodes will be written to machinefile advert-launcher-machines-<jobid>
            this method is only call by background thread and thus not threadsafe"""
        number_nodes = int(job_dir.get_attribute("NumberOfProcesses"))
        nodes = []
        if (len(self.freenodes)>=number_nodes):
            unique_nodes=set(self.freenodes)
            for i in unique_nodes:
                number = self.freenodes.count(i)
                print "allocate: " + i + " number nodes: " + str(number)
                for j in range(0, number):
                    if(number_nodes > 0):
                        nodes.append(i)
                        self.freenodes.remove(i)                
                        self.busynodes.append(i)
                        number_nodes = number_nodes - 1
                    else:
                        break

            machine_file_name = self.get_machine_file_name(job_dir)
            machine_file = open(machine_file_name, "w")
            #machine_file.writelines(self.freenodes[:number_nodes])
            machine_file.writelines(nodes)
            machine_file.close() 
            print "wrote machinefile: " + machine_file_name + " Nodes: " + str(nodes)
            # update node structures
            #self.busynodes.extend(self.freenodes[:number_nodes])
            #del(self.freenodes[:number_nodes])            
            return machine_file_name
        return None
    
    def print_machine_file(self, filename):
         fh = open(filename, "r")
         lines = fh.readlines()
         fh.close
         print "Machinefile: " + filename + " Hosts: " + str(lines)
         
    def free_nodes(self, job_dir):
         print "Free nodes ..."
         number_nodes = int(job_dir.get_attribute("NumberOfProcesses"))
         machine_file_name = self.get_machine_file_name(job_dir)
         print "Machine file: " + machine_file_name
         allocated_nodes = ["localhost"]
         try:
                 machine_file = open(machine_file_name, "r")
                 allocated_nodes = machine_file.readlines()
                 machine_file.close()
         except:
             pass
         for i in allocated_nodes:
             self.busynodes.remove(i)
             self.freenodes.append(i)
         print "Delete " + machine_file_name
         os.remove(machine_file_name)
               
            
    def get_machine_file_name(self, job_dir):
        """create machinefile based on jobid"""
        job_dir_url =job_dir.get_url().get_string()        
        job_dir_url = job_dir_url[(job_dir_url.rindex("/", 0, len(job_dir_url)-1)+1)
                                  :(len(job_dir_url)-1)]        
        homedir = os.path.expanduser('~')
        return homedir  + "/advert-launcher-machines-"+ job_dir_url
        
    def poll_jobs(self):
        """Poll jobs from advert service. """
        jobs = []
        #try:
        jobs = self.base_dir.list()
        print "Found " + "%d"%len(jobs) + " jobs in " + str(self.base_dir.get_url().get_string())
        #except:
        #    pass
        for i in jobs:  
            #print i.get_string()
            job_dir = None
            try: #potentially racing condition (dir could be already deleted by RE-Manager
                job_dir = self.base_dir.open_dir(i.get_string(), saga.advert.Create | saga.advert.ReadWrite)
            except:
                pass
            if job_dir != None:
                self.execute_job(job_dir)  
    
    def read_energy(self, replica_id):
        enfile = open(str(replica_id) + "/stdout-" + str(replica_id) + ".txt")
        lines = enfile.readlines()
        for line in lines:
          items = line.split()
          if len(items) > 0:
            if items[0] in ("ENERGY:"):
               en = items[11]
        print "(DEBUG) energy : " + str(en) 
        return en 
 
    def read_temp(self, replica_id):
        enfile = open(str(replica_id) + "/stdout-" + str(replica_id) + ".txt")
        lines = enfile.readlines()
        for line in lines:
          items = line.split()
          if len(items) > 0:
            if items[0] in ("ENERGY:"):
               en = items[12]
        print "(DEBUG) energy : " + str(en)
        return en

    def monitor_jobs(self):
        """Monitor running processes. """ 
        for i in self.jobs:
            if self.processes.has_key(i): # only if job has already been starteds
                p = self.processes[i]
                p_state = p.poll()
                print self.print_job(i) + " state: " + str(p_state)
                if (p_state != None and (p_state==0 or p_state==255)):
                    print self.print_job(i)  + " finished. "
                   # i.set_attribute("state", str(saga.job.Done))                 
                    rid=i.get_attribute("replica_id")
                   # en = self.read_energy(str(rid))  
                   # i.set_attribute("energy", str(en))##
                   # temp = self.read_temp(str(rid))  
                   # i.set_attribute("temp", str(temp))
                   # time.sleep(1)
                    i.set_attribute("state", str(saga.job.Done))
                    self.free_nodes(i)
                    del self.processes[i]
                elif p_state!=0 and p_state!=255 and p_state != None:
                    print self.print_job(i) + " failed.  "
                    # do not free nodes => very likely the job will fail on these nodes
                    # self.free_nodes(i)
                    del self.processes[i]
                    if self.restarted.has_key(i)==False:
                        print "Try to restart job " + self.print_job(i)
                        self.restarted[i]=True
                        self.execute_job(i)
                    else:
                        print "do not restart job " + self.print_job(i)
                        i.set_attribute("state", str(saga.job.Failed))
     
    def print_job(self, job_dir):
        return  "Job: " + job_dir.get_url().get_string() + " Working Dir: " + job_dir.get_attribute("WorkingDirectory") + " Excutable: " + job_dir.get_attribute("Executable")
                                
    def monitor_checkpoints(self):
        """ parses all job working directories and registers files with Migol via SAGA/CPR """
        #get current files from AIS
        url = saga.url("advert_launcher_checkpoint");
        checkpoint = saga.cpr.checkpoint(url);
        files = checkpoint.list_files()
        for i in files:
            print i      
        dir_listing = os.listdir(os.getcwd())
        for i in dir_listing:
            filename = dir+"/"+i
            if (os.path.isfile(filename)):
                if(check_file(files, filename==False)):
                      url = self.build_url(filename)
                      print str(self.build_url(filename))
                        
    def build_url(self, filename):
        """ build gsiftp url from file path """
        hostname = socket.gethostname()
        file_url = saga.url("gsiftp://"+hostname+"/"+filename)
        return file_url
                
    def check_file(self, files, filename):
        """ check whether file has already been registered with CPR """
        for i in files:
            file_path = i.get_path()
            if (filename == filepath):
                return true
        return false
                        
    def start_background_thread(self):
        self.stop=False
        print "####################" + time.asctime(time.localtime(time.time())) + "##################"
        print "##################################### New POLL/MONITOR cycle ##################################"
        print "Free nodes: " + str(len(self.freenodes)) + " Busy Nodes: " + str(len(self.busynodes))
        while True and self.stop==False:
            if self.base_dir.exists(self.base_url) == False:
                print "Job dir deleted - terminate agent"
                break
            else:
                print "Job dir: " + str(self.base_dir) + "exists."

            try:
                self.poll_jobs()
                self.monitor_jobs()            
                #time.sleep(5)
                self.failed_polls=0
            except saga.exception:
                traceback.print_exc(file=sys.stdout)
                self.failed_polls=self.failed_polls+1
                if self.failed_polls>3: # after 3 failed attempts exit
                    break

    def stop_background_thread(self):        
        self.stop=True
    
    def init_cpr(self):
        # init cpr
        self.js=None
        if self.CPR == True:
            try:
                print "init CPR monitoring for Agent"
                js = saga.cpr.service()
            except:
                sys.exc_traceback

#########################################################
#  main                                                 #
#########################################################
if __name__ == "__main__" :
    args = sys.argv
    num_args = len(args)
    if (num_args!=3):
        print "Usage: \n " + args[0] + " <advert-host> <advert-director>"
        sys.exit(1)
    
    bigjob_agent = bigjob_agent(args)    
    
