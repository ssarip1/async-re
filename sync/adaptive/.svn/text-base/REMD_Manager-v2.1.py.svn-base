""" The main script for REMDgManager (w/o migol) : REMDManager-v2.1.py 
2008/8/26

Usage : (For Test_RE)   python REMDManager-v2.1.py  
                     or python REMDManager-v2.1.py --type=Test_RE
        (For REMD) python REMDManager-v2.1.py --type=REMD --configfile=re_manager_v1.conf

jhkim at cct dot lsu dot edu

(Note on the current stage)
1. Currently, the following simple scenario is assumed.
a. each replica is submitted to each local scheduler via CPR/Migol  (will be changed with the major revision) (again not for this version)
b. remote directories should exist at each HPC (see remd_manager.config)
c. There is a wrapper in HPC to register checkpoint files to migol (all file are registered as checkpoint files) (again not for this version)
d. This version does not have migol  part.
e. remd-temp.out will contain temperature trajectories of each replica 
f. NPT.cof is modified before being sent to HPC
g. "output.txt" is the output file name with which energy is pulled out for the replica exchange step
h. get_energy(), do_exchange(), prepare_NAMD_config() might be changed later since implementaion is not optimized yet
: energy search in output.txt is not efficient as the size of output.txt grows, prepare_NAMD_config() needs more for assigning other initial files 

"""

import sys, os, os.path, random, time
import optparse
import logging
import saga
import re
import math
import threading
import traceback
import advert_job
import pdb

""" Config parameters (will be moved to config file in the future) """
CPR = False 
SCP = False
GlideIn = True
AdaptiveSampling = True

########################################################
#  Global variable 
########################################################
class RE_INFO (object):
    """ 
    This class holds information about the application and replicas remotely running via SAGA-CPR/MIGOL
    
    """
    
    def __init__(self):
        # general info
        self.app_name = "NAMD"
        self.stage_in_files = []
        self.numberofprocesses = 8
        self.exchange_count = 0
        self.totalcputime = '40'
        self.arguments = []
        
        # lists for variables of each replica (Note that these variable should have n variables where n is self.replica_count
        self.replica_count = 0
        self.remote_hosts = []
        self.gridftp_hosts = []
        self.remote_host_local_schedulers = []
        self.projects = []
        self.queues = []
        self.workingdirectories = []
        self.executables = []
        self.temperatures = []
        
        # instant variable for replica exchange
        self.replica_saga_jobs = []   # saga jobs
        self.istep = 0
        self.iEX = 0
        
        # advert host
        self.advert_host ="localhost"
        # map <host, glidin-job>
        self.advert_glidin_jobs = {}
        self.userproxy = []        
        self.number_glideins_per_host = 1
#####################################
#  Elementary Functions
########################################################

def set_saga_job_description(replica_ID, RE_info, iflag):
    
    if CPR == True and GlideIn == False: #if glidein use normal jd
        jd = saga.cpr.description()
        #jd.spmdvariation == "single" # Launch via NAMD-Launcher
    else:    
        jd = saga.job.description()
    jd.spmd_variation = "mpi" # launch MPI directly
    jd.number_of_processes = RE_info.numberofprocesses
    jd.total_cpu_time = RE_info.totalcputime
    jd.arguments = RE_info.arguments
    jd.executable = RE_info.executables[replica_ID]
    #jd.queue = RE_info.projects[replica_ID] + "@" + RE_info.queues[replica_ID]
    jd.queue = RE_info.queues[replica_ID]
    jd.working_directory = RE_info.workingdirectories[replica_ID]
    project = []
    project.append(RE_info.projects[replica_ID])
    jd.job_project = project
    
    jd.output = "output.txt"    #this is requried for Migol
    jd.error = "error.txt"
    
    return jd


def file_stage_in_with_saga(input_file_list_with_path, remote_machine_ip, remote_dir, RE_info):
    userproxy=None
    try: 
        userproxy = RE_info.userproxy[RE_info.remote_hosts.index(remote_machine_ip)] 
    except:
        try:
            userproxy = RE_info.userproxy[RE_info.gridftp_hosts.index(remote_machine_ip)]
        except:
            pass
    if userproxy != None or userproxy=="":
         os.environ["X509_USER_PROXY"]=userproxy
         print "use proxy: " + userproxy
    else:
         print "use standard proxy"
    for ifile in input_file_list_with_path:

        if remote_machine_ip.find('localhost') >= 0:
            dest_url_str = 'file://'
        else:
            dest_url_str = 'gridftp://'+remote_machine_ip + "/"
        source_url_str = 'file://'
        print "stage file: " + ifile + " to " + dest_url_str

        ifile_basename = os.path.basename(ifile)
        if not os.path.isfile(ifile):
            error_msg = "Input file %s does not exist in %s"%(ifile_basename, os.path.dirname(ifile))
            logging.error(error_msg)
        else:
                
            try:
                source_url_str = source_url_str+ifile
                dest_url_str = dest_url_str + os.path.join(remote_dir, ifile_basename)
                source_url = saga.url(source_url_str)
                dest_url = saga.url(dest_url_str)
                print "stage file: " + source_url_str + " to " + dest_url_str

                sagafile = saga.filesystem.file(source_url)
                sagafile.copy(dest_url)
                logging.info("Now Input file %s is staged into %s"%(ifile_basename,dest_url_str))
            except saga.exception, e:
                error_msg = "Input file %s failed to be staged in"%(ifile_basename)
                logging.error(error_msg)
                
    return None
    
""" state files with SCP
    This function assumes that public/key authentication for SCP is enabled """
def file_stage_in_with_scp(input_file_list_with_path, remote_machine_ip, remote_dir):
    for ifile in input_file_list_with_path:
        print "stage file: " + ifile
        dest_url_str = remote_machine_ip + ":"
    
        ifile_basename = os.path.basename(ifile)
        if not os.path.isfile(ifile):
            error_msg = "Input file %s does not exist in %s"%(ifile_basename, os.path.dirname(ifile))
            logging.error(error_msg)
        else:
            try:
                source_url_str = ifile
                dest_url_str = dest_url_str + os.path.join(remote_dir, ifile_basename)
                command = "scp " + source_url_str + " " + dest_url_str
                print "Execute: " + command
                #os.popen(command)
                logging.info("Now Input file %s is staged into %s"%(ifile_basename,dest_url_str))
            except saga.exception, e:
                error_msg = "Input file %s failed to be staged in"%(ifile_basename)
                logging.error(error_msg)
    
    return None
                
                    

def file_stage_out_with_saga(file_list, local_dir, remote_machine_ip, remote_dir):
    for ifile in file_list:
        try:
            source_url_str = "gsiftp://"+remote_machine_ip + "/" + os.path.join(remote_dir, ifile)
            dest_url_str = "file://" + local_dir + "/" + ifile
# for the time being, use globus-url-copy            
#            source_url = saga.url(source_url_str)
#            dest_url = saga.url(dest_url_str)
 
#            sagafile = saga.file.file(source_url)
#            sagafile.copy(dest_url)
            print "(DEBUG) Now I am pulling the output.txt file at %s to %s"%(source_url_str, dest_url_str)
            cmd = "globus-url-copy %s %s"%(source_url_str, dest_url_str) 
            os.system(cmd)    
        except saga.exception, e:
            error_msg = "File stage out failed: "+ source_url_str

    return None
    
    
def submit_job(dest_url_string, jd):
    error_string = ""
    js = saga.job.service(saga.url(dest_url_string))
    new_job = js.create_job(jd)
    new_job.run()
    return error_string, new_job

def submit_job_cpr(dest_url_string, jd, checkpt_files):
    error_string = ""
    start = time.time()
    js = saga.cpr.service(saga.url(dest_url_string))
    jd_start = jd
    jd_restart = jd
    
    #here checkpoint files are registered (not done yet)
#    check_point = saga.cpr.checkpoint("REMD_MANAGER_CHECKPOINT")
    
#    for ifile in checkpt_files:
#        check_point.add_files(ifile)
    
    new_cpr_job = js.create_job(jd_start, jd_restart)
    new_cpr_job.run()
    print "job state: " + str(new_cpr_job.get_state());
    print "spawning time " + "%d"%(time.time()-start) +" s"    
    return error_string, new_cpr_job


def prepare_NAMD_config(irep, RE_info):
# The idea behind this is that we can simply modify NPT.conf before submit a job to set temp and other variables
    
    ifile = open("NPT.conf")   # should be changed if a different name is going to be used
    lines = ifile.readlines()
    
    for line in lines:
        if line.find("desired_temp") >= 0 and line.find("set") >= 0:
            items = line.split()
            temp = items[2]
            if eval(temp) != RE_info.temperatures[irep]:
                print "\n (DEBUG) temperature is changing to " + str(RE_info.temperatures[irep]) + " from " + temp + " for rep" + str(irep)+time.asctime(time.localtime(time.time()))
                lines[lines.index(line)] = "set desired_temp %s \n"%(str(RE_info.temperatures[irep]))

    ifile.close() 
    
    ofile = open("NPT.conf","w")
    for line in lines:    
        ofile.write(line)
    ofile.close()
          


def get_energy(replica_ID, RE_info):
#I know This is not the best one!  namd output is staged out and take the energy out from the file
 
    file_list = ["output.txt"]  
    local_dir = os.getcwd()
    remote_machine_ip = RE_info.remote_hosts[replica_ID]
    if len(RE_info.gridftp_hosts)>0:
               remote_machine_ip = RE_info.gridftp_hosts[replica_ID]
    remote_dir = RE_info.workingdirectories[replica_ID]

    file_stage_out_with_saga(file_list, local_dir, remote_machine_ip, remote_dir)
   
    enfile = open("output.txt", "r")
    lines = enfile.readlines()
    for line in lines:
        items = line.split()
        if len(items) > 0:
            if items[0] in ("ENERGY:"):
                en = items[11]  
    print "(DEBUG) energy : " + str(en) + " from replica " + str(replica_ID) 
    
    return eval(en)

def do_exchange(energy, irep, jrep, RE_info):
    iflag = False
    en_a = energy[irep]
    en_b = energy[jrep]
    
    factor = 0.0019872  # from R = 1.9872 cal/mol
    delta = (1./RE_info.temperatures[irep]/factor - 1./RE_info.temperatures[irep+1]/factor)*(en_b-en_a)
    if delta < 0:
        iflag = True
    else :
        if math.exp(-delta) > random.random() :
            iflag = True

    if iflag is True:
        tmpNum = RE_info.temperatures[jrep]
        RE_info.temperatures[jrep] = RE_info.temperatures[irep]
        RE_info.temperatures[irep] = tmpNum

    print "(DEBUG) delta = %f"%delta + " en_a = %f"%en_a + " from rep " + str(irep) + " en_b = %f"%en_b +" from rep " + str(jrep)


def submit_job_advert(RE_info, dest_url_string, jd):
    """ submit job via advert service and glidin job"""
    error_string = ""
    new_advert_job = advert_job.advert_job(RE_info.advert_host)
    new_advert_job = new_advert_job.submit_job(dest_url_string, jd)
    return error_string, new_advert_job

#########################################################
#  Initialize
#########################################################
def initialize(config_filename):
    
    RE_info = RE_INFO()
    
    conf_file = open(config_filename)
    lines = conf_file.readlines()
    conf_file.close()
    # config file should have the following format. In brief, : is needed between the keyword and variable(s) where 
    # multi variables are separated by spaces 
    # exeutables :  /usr/local/namd  /usr/local/namd 
    # arguments : NPT.conf 
    
    for line in lines:
        print line
        items = line.split()
        if line.find(':'):
            key = items[0]
            value = items[2:]
            
            # list of variables for each replica
            if key == 'remote_host':
                for ihost in value:
                    RE_info.remote_hosts.append(ihost)
            
            elif key == 'gridftp_host':
                for ihost in value:
                    RE_info.gridftp_hosts.append(ihost)
                    
            elif key == 'number_glideins_per_host':
                RE_info.number_glideins_per_host = eval(value[0])
                    
            elif key == 'userproxy':
                for up in value:
                    up = up.rstrip("\"")
                    up = up.lstrip("\"")
                    RE_info.userproxy.append(up)
    
            elif key == 'remote_host_local_scheduler':
                for isched in value:
                    RE_info.remote_host_local_schedulers.append(isched)
            
            elif key == 'workingdirectory':   # this is the list of working directories for each replica in order with remote_hosts
                for idir in value:
                    RE_info.workingdirectories.append(idir)
            
            elif key == 'executable':
                for ival in value:
                    print "Executable: " + ival
                    RE_info.executables.append(ival)
                
            elif key == 'queue':
                for ival in value:
                    RE_info.queues.append(ival)
    
            elif key == 'project':     
                for ival in value:
                    RE_info.projects.append(ival)      
 
                    # variables common to all replicas
            elif key == 'arguments':
                # support quoted arguments like this:
                # "/usr/local/packages/namd-2.6-mvapich-1.0-intel10.1/namd2 NPT.conf" "mpi"
                args = line.split(":")[1]
                p = re.compile(r'\" \"|\"')
                args_parts = p.split(args)
                for ival in args_parts:
                    if (ival.strip() !=""):
                        print "add arg: " + ival
                        RE_info.arguments.append(ival.strip())  
                #for ival in value:
                #    RE_info.arguments.append(ival)      
 
            elif key == 'totalcputime':
                RE_info.totalcputime = value[0]    
                
            elif key == 'numberofprocesses':
                RE_info.numberofprocesses = value[0] 
            

            elif key == 'replica_count':
                RE_info.replica_count = eval(value[0])
                
            elif key == 'exchange_count' :
                RE_info.exchange_count = eval(value[0])    
                
            elif key == "stage_in_file" :
                cwd = os.getcwd()
                for ifile in value:
                    ifile_with_path = cwd + "/" + ifile    # of course unix machine
                    RE_info.stage_in_files.append(ifile_with_path)      
            elif key == "temperature" :
                for itemp in value:
                    RE_info.temperatures.append(eval(itemp))        
            elif key == "advert_host" :
                RE_info.advert_host = value[0] 
            else :
                logging.info("this line %s in %s does not have configure variables"%(line, config_filename))    
            
                
        else :
            logging.info("this line %s in %s does not have configure variables"%(line, config_filename))
                         
    random.seed(time.time()/10.)
    
    return RE_info

def start_job(RE_info, irep):
    """ start job:
        if glidin job has been started via advert job start protocol
        otherwise with SAGA CPR/Job """
    jd = set_saga_job_description(irep, RE_info)
    host = RE_info.remote_hosts[irep]
    scheduler = RE_info.remote_host_local_schedulers[irep]
    print "start job at: " + host   
    
    new_job=None
    if (RE_info.advert_glidin_jobs.has_key(host)): # start via advert service
        glidin_url = RE_info.advert_glidin_jobs[host].glidin_url 
        error, new_job =  submit_job_advert(RE_info, glidin_url, jd)    
    else: # normal SAGA CPR/Job start
        if (CPR==True):
            dest_url_string = "migol://" + host + "/" + "jobmanager-" + scheduler     # just for the time being
            error, new_job = submit_job_cpr(dest_url_string, jd, None)
        else:
            dest_url_string = "gram://" + host + "/" + "jobmanager-" + scheduler    # just for the time being
            error, new_job = submit_job(dest_url_string, jd)
    RE_info.replica.insert(irep,new_job)
    print "Replica " + "%d"%irep + " started." 
    
      
def start_glidin_jobs(RE_info):
    """start glidin jobs (advert_job.py) at every unique machine specified in RE_info"""  
    unique_hosts = set(RE_info.remote_hosts)    
    for i in unique_hosts:
        print "Number hosts: " + str(RE_info.remote_hosts.count(i)) + " Number processes per job: " + str(RE_info.numberofprocesses) + " Number GlideIns per Host: " + str(RE_info.number_glideins_per_host) + " Index: " + str(RE_info.remote_hosts.index(i))
        nodes = int(RE_info.remote_hosts.count(i)) * int(RE_info.numberofprocesses) 
        lrms = RE_info.remote_host_local_schedulers[RE_info.remote_hosts.index(i)]
        project = RE_info.projects[RE_info.remote_hosts.index(i)]
        queue = RE_info.queues[RE_info.remote_hosts.index(i)]
        workingdirectory = RE_info.workingdirectories[RE_info.remote_hosts.index(i)]
        userproxy=None
        try:
            userproxy = RE_info.userproxy[RE_info.remote_hosts.index(i)]
        except:
            pass
        if(CPR==True):
            lrms_url = "migol://"    
        else:
            lrms_url = "gram://"
        lrms_url = lrms_url + i + "/" + "jobmanager-" + lrms      
        nodes_per_glidein = nodes
        num_glidein = RE_info.number_glideins_per_host
        if (num_glidein != None and num_glidein != 0):
            nodes_per_glidein = nodes/num_glidein # must be divisible 
            print "Glidin URL: " + lrms_url    
            print "hosts: " + str(i) + " number of replica_processes (total): " + str(nodes)
            print "number glide-ins: " + str(num_glidein) + " node per glidein: " + str(nodes_per_glidein)
            print "Project: " + project + " Queue: " + queue + " Working Dir: " +workingdirectory
        
            # start job
            for ng in range(0, RE_info.number_glideins_per_host):
                advert_glidin_job = advert_job.advert_glidin_job(RE_info.advert_host)
                advert_glidin_job.start_glidin_job(lrms_url, 
                                               None,
                                               nodes_per_glidein,
                                               queue,
                                               project,
                                               workingdirectory, userproxy, None)
                if RE_info.advert_glidin_jobs.has_key(i) == False:
                    RE_info.advert_glidin_jobs[i] = []
                RE_info.advert_glidin_jobs[i].append(advert_glidin_job);
                print "Started: " + str(advert_glidin_job)  + " Glide-In Job Number: " + str(ng)+ time.asctime(time.localtime(time.time()))

def stop_glidin_jobs(RE_info):
    for i in RE_info.advert_glidin_jobs.items():        
        for job in i[1]:
            job.cancel()
        
def transfer_files(RE_info, irep):
    start = time.time()
    host = RE_info.remote_hosts[irep]
    remote_machine_ip = RE_info.remote_hosts[irep]
    if len(RE_info.gridftp_hosts)>0:
            remote_machine_ip = RE_info.gridftp_hosts[irep]

    remote_dir = RE_info.workingdirectories[irep]
    prepare_NAMD_config(irep, RE_info)
    if SCP == True:
            pass
            file_stage_in_with_scp(RE_info.stage_in_files, remote_machine_ip, remote_dir)
    else:
            file_stage_in_with_saga(RE_info.stage_in_files, remote_machine_ip, remote_dir, RE_info)
    print "(INFO) Replica %d : Input files are staged into %s  "%(irep, remote_machine_ip)
    print "time to stag files: " + str(time.time()-start)

def check_glidein_states(RE_info, current_replica_id_glidein_dict, start_glidin):
    """ check for Glide-In states
        returns dictionary: <replica_id, glidein_url> 
        only keys from replicas with active glidein job are set."""
    # query glidin job states and cache them into a dict.
    replica_id_glidein_dict = {}
    if GlideIn == True:
        # divide replica chunks
        unique_hosts = list(set(RE_info.remote_hosts))
        print "Number unique hosts: " + str(len(unique_hosts)) + time.asctime(time.localtime(time.time()))
        for i in range(0, len(unique_hosts)):
            host = unique_hosts[i]
            num_rep_per_host = int(RE_info.remote_hosts.count(host)) 
            num_replica_per_glidein = num_rep_per_host
            if RE_info.number_glideins_per_host > 1:
                num_replica_per_glidein = num_rep_per_host/RE_info.number_glideins_per_host
            
            glidin_jobs = RE_info.advert_glidin_jobs[host]      
            print "Host: " + host + " Number Glide-Ins: " + str(len(glidin_jobs)) + " Number Replicas: " + str(num_rep_per_host)
            replica_id = RE_info.remote_hosts.index(host)
            for j in range(0, len(glidin_jobs)):
                state = glidin_jobs[j].get_state_detail()
                glidin_url = glidin_jobs[j].glidin_url 
                print "glidein: " + glidin_url + " state: " + state
                if state.lower()== "running":
                    # distribute replicas to glideins by setting glideinurl to replicaid
                    for r in range(0, num_replica_per_glidein):
                        #replica_id = (i*num_rep_per_host) + (j*num_replica_per_glidein) + r
                        if current_replica_id_glidein_dict.has_key(replica_id)==False:
                            print "Glide-In: " + glidin_url + " got active after: " + str(time.time()-start_glidin) + " s"
                        print "set replica id: " + str(replica_id) + " glidein: " + glidin_url + " state: running"
                        replica_id_glidein_dict[replica_id]=glidin_url
                        replica_id = replica_id + 1
                else:
                    replica_id = replica_id + num_replica_per_glidein
    return replica_id_glidein_dict

def check_glidein_for_replica(RE_info, current_replica_id_glidein_dict, irep):
    """ tests whether glidein for replica irep has been started 
        returns true if started
        otherwise false """
    all_glideins_ready = False
    if len(current_replica_id_glidein_dict.keys())==int(RE_info.replica_count):
                all_glideins_ready = True
    host = RE_info.remote_hosts[irep]
    if current_replica_id_glidein_dict.has_key(irep):
        print "Replica: "+ str(irep) +  " Glidin job on host: " + host + "state: " + str(current_replica_id_glidein_dict[irep]).lower()
    else:
        print "Replica: "+ str(irep) +  " Glidin job on host: " + host + "state: not running. "

    """ checks whether glidein for replica has started """
    replica_is_ready =  ((GlideIn == True) and 
                        ((current_replica_id_glidein_dict.has_key(irep) and AdaptiveSampling==True) or all_glideins_ready == True))
    return replica_is_ready

#########################################################
#  run_REMDg
#########################################################
def run_REMDg(configfile_name):
    """ Main loop running replica-exchange """
    start = time.time()
    # initialize of all replica and its related variables
    RE_info = initialize(configfile_name)
    numEX = RE_info.exchange_count    
    ofilename = "remd-temp.out"
    if GlideIn == True:
        print "Start with glidein"+ time.asctime(time.localtime(time.time()))
        start_glidin_jobs(RE_info)
    else:
        print "Start without glidein"

    ###################################### file staging ################################################
    for irep in range(0, RE_info.replica_count):
           host = RE_info.remote_hosts[irep]
           remote_machine_ip = RE_info.remote_hosts[irep]
           if len(RE_info.gridftp_hosts)>0:
                  remote_machine_ip = RE_info.gridftp_hosts[irep]

           remote_dir = RE_info.workingdirectories[irep]
           prepare_NAMD_config(irep, RE_info)
           if SCP == True:
               file_stage_in_with_scp(RE_info.stage_in_files, remote_machine_ip, remote_dir)
           else:
               file_stage_in_with_saga(RE_info.stage_in_files, remote_machine_ip, remote_dir, RE_info)
           print "(INFO) Replica %d : Input files are staged into %s  "%(irep, remote_machine_ip)
    
    iEX = 0
    total_number_of_namd_jobs = 0
    current_replica_id_glidein_dict={}
    while 1:
        print "\n"
        # reset replica number
        numReplica = RE_info.replica_count
        current_replica_id_glidein_dict = check_glidein_states(RE_info, current_replica_id_glidein_dict, start)
        if len(current_replica_id_glidein_dict.keys())==numReplica:
                all_glideins_ready = True
                print "All Glide-Ins got active after: " + str(time.time()-start) + " s"
        ####################################### NPT staging ################################################
        for irep in range(0,numReplica):
            host = RE_info.remote_hosts[irep]
            #if replica_id_glidein_dict.has_key(irep):
            #    print "Replica: "+ str(irep) +  " Glidin job on host: " + host + "state: " + str(replica_id_glidein_dict[irep]).lower()
            #else:
            #    print "Replica: "+ str(irep) +  " Glidin job on host: " + host + "state: not running. "
            # only start replicas if glidin job is running
            if GlideIn==False or check_glidein_for_replica(RE_info, current_replica_id_glidein_dict, irep)==True:
               remote_machine_ip = RE_info.remote_hosts[irep]
               if len(RE_info.gridftp_hosts)>0:
                   remote_machine_ip = RE_info.gridftp_hosts[irep]
               remote_dir = RE_info.workingdirectories[irep]
               # prepare parameter 
               prepare_NAMD_config(irep, RE_info) 
               if SCP == True:
                   file_stage_in_with_scp([os.getcwd()+"/NPT.conf"], remote_machine_ip, remote_dir)
               else:
                   file_stage_in_with_saga([os.getcwd()+"/NPT.conf"], remote_machine_ip, remote_dir, RE_info) 
                   print "(INFO) Replica %d : Input files are staged into %s  "%(irep, remote_machine_ip) 
            else:
                print "Glidin job on host: " + host + " not ready ... not stage files"
                
        ####################################### replica job spawning #######################################  
        # job submit   
        RE_info.replica = []
        start_time = time.time()
        job_url_irep_map = {}
        for irep in range(0,numReplica):
            host = RE_info.remote_hosts[irep]
            print "check host: " + str(host)
            if check_glidein_for_replica(RE_info, current_replica_id_glidein_dict, irep)==True:
                jd = set_saga_job_description(irep, RE_info, "")
                dest_url_string = "gram://" + host + "/" + "jobmanager-" + RE_info.remote_host_local_schedulers[irep]     # just for the time being
                checkpt_files = []     # will be done by migol not here  (JK  08/05/08)
                glidin_url = current_replica_id_glidein_dict[irep] 
                error_msg, new_job = submit_job_advert(RE_info, glidin_url, jd)
                RE_info.replica.append(new_job)
                job_url_irep_map[new_job]=irep
                print "(INFO) Replica " + "%d"%irep + " started (Num of Exchange Done = %d)"%(iEX)
            elif GlideIn==False:
                jd = set_saga_job_description(irep, RE_info, "")
                if CPR == True:
                    dest_url_string = "migol://" + host + "/" + "jobmanager-" + RE_info.remote_host_local_schedulers[irep]     # just for the t
                    error_msg, new_job = submit_job_cpr(dest_url_string, jd, None)
                else:
                    dest_url_string = "gram://" + host + "/" + "jobmanager-" + RE_info.remote_host_local_schedulers[irep]     # just for the time being
                    error_msg, new_job = submit_job(dest_url_string, jd)
                RE_info.replica.append(new_job)
            else:
                  print "Glidin job on host: " + host + " not ready ... not start replica"
           # time.sleep(0.05)

        end_time = time.time()        
        # contains number of started replicas
        numReplica = len(RE_info.replica)
        if numReplica == 0: # no replica process started
           # time.sleep(10)
            continue # next attempt to start replica processes

        print "started " + "%d"%numReplica + " of " + "%d"%RE_info.replica_count + " in this round." 
        print "Time for spawning " + "%d"%numReplica + " replica: " + str(end_time-start_time) + " s"
        ####################################### Wating for job termination ###############################
        # job monitoring step
        energy = [0 for i in range(0, numReplica)]
        flagJobDone = [ False for i in range(0, numReplica)]
        numJobDone = 0

        print "\n\n" 
        while 1:    
            print "\n##################### Replica State Check at: " + time.asctime(time.localtime(time.time())) + " ########################"
            for i in range(0, numReplica):
                running_job = RE_info.replica[i]
                irep = i 
                if job_url_irep_map.has_key(running_job):
                    irep = job_url_irep_map[running_job]
                print "Job index: " + str(irep)
                try: 
                    state = running_job.get_state()
                except:
                    pass
                print "job: " + str(running_job) + " received state: " + str(state)
                if (str(state) == "Done") and (flagJobDone[i] is False) :   
                    print "(INFO) Replica " + "%d"%irep + " done"
                    energy[i] = get_energy(irep, RE_info)
                    flagJobDone[i] = True
                    numJobDone = numJobDone + 1
                    total_number_of_namd_jobs = total_number_of_namd_jobs + 1
                elif(str(state)=="Failed"):
                    stop_glidin_jobs(RE_info)
                    sys.exit(1)
            
            if numJobDone == numReplica:
                    break
           # time.sleep(5)

        ####################################### Replica Exchange ##################################    
        # replica exchange step        
        print "\n(INFO) Now exchange step...."

        for irep in range(0, numReplica-1):
            en_a = energy[irep]
            en_b = energy[irep+1]
            do_exchange(energy, irep, irep+1, RE_info)

        iEX = iEX +1
        RE_info.iEX = iEX
        output_str = "%5d-th EX :"%iEX
        for irep in range(0, numReplica):
            output_str = output_str + "  %5d"%RE_info.temperatures[irep]
        
        print "\n\nExchange result : "
        print output_str + "\n\n"
        
        ofile = open(ofilename,'a')
        for irep in range(0, numReplica):
            ofile.write(" %d"%(RE_info.temperatures[irep]))
        ofile.write(" \n")            
        ofile.close()

        if iEX == numEX:
            break

        ########################## delete old jobs #####################
        if GlideIn == True:    
            for i in RE_info.replica:
                i.delete_job()
        

    print "REMD Runtime: " + str(time.time()-start) + " s; Glide-In: " + str(GlideIn) + "; number Glide-In per host: " + str(RE_info.number_glideins_per_host)  + " number replica: " + str(RE_info.replica_count) + "; CPR: " + str(CPR)+ "; number namd jobs: " + str(total_number_of_namd_jobs)
    # stop gliding job        
    stop_glidin_jobs(RE_info)


#########################################################
#  run_test_RE
#########################################################    
def run_test_RE(nReplica, nRand):
    
    # initialization
    app_name = "test_app.py"
    numReplica = nReplica
    numRand = nRand        
    
    curr_working_dir = os.getcwd()
    ofilenamestring = "test-%d.out"
    ofilenamelist = [ofilenamestring%i for i in range (0,numReplica)]
    replicaListing = [i for i in range(1, numReplica+1)]
    numEX = 5
    iEX = 0
    
    
    while 1:
        # job scheduling and job submission step
        job_list = []
        for ireplica in range(0,numReplica):
            js = saga.job.service(saga.url("fork://localhost%s"%curr_working_dir))
            jd = saga.job.description()
            jd.executable = app_name
            jd.arguments=[str(replicaListing[ireplica]),ofilenamelist[ireplica], str(numRand)]
            new_job = js.create_job(jd)
            job_list.append(new_job)
            new_job.run()
            print "Replica " + "%d"%replicaListing[ireplica] + " started (Run = %d)"%(iEX+1)

        # job monitoring step
        rnumber = [0 for i in range(0, numReplica)]
        flagJobDone = [ False for i in range(0, numReplica)]
        numJobDone = 0
        while 1:    
            for ireplica in range(0, numReplica):
                running_job = job_list[ireplica]
                state = running_job.get_state()
                if (str(state) == "Done") and (flagJobDone[ireplica] is False) :   
                    print "Replica " + "%d"%(ireplica+1) + " done"
                    ifile = open(ofilenamelist[ireplica])
                    lines = ifile.readlines()
                    ifile.close()
                    lastline = lines[-1]
                    
                    items = lastline.split()
                    rnumber[ireplica] = eval(items[1])
                    flagJobDone[ireplica] = True
                    numJobDone = numJobDone + 1
                else :
                    print "sleep 5 seconds"
                  #  time.sleep(5)
                    pass
            
            if numJobDone == numReplica:
                break
            
        # replica exchange step        
        for ireplica in range(0, numReplica-1):
            ran_a = rnumber[ireplica]
            ran_b = rnumber[ireplica+1]
            if ran_a < ran_b :
                tmpNum = replicaListing[ireplica+1]
                replicaListing[ireplica+1] = replicaListing[ireplica]
                replicaListing[ireplica] = tmpNum
            else :
                pass
    
        iEX = iEX +1
        output_str = "%5d-th EX :"%iEX
        for ireplica in range(0, numReplica):
            output_str = output_str + "  %3d"%replicaListing[ireplica]
        
        print "\n\nExchange result : "
        print output_str + "\n\n"
                    
        if iEX == numEX:
            break

    
#########################################################
#  main
#########################################################

if __name__ == "__main__" :
    start = time.time()
    op = optparse.OptionParser()
    op.add_option('--type','-t')
    op.add_option('--configfile','-c')
    op.add_option('--numreplica','-n',default='2')
    options, arguments = op.parse_args()

    # enable monitoring through Migol
    js=None
    if(CPR==True):
        js = saga.cpr.service()
    
    # AL: I disabled this option temporarly since it is not working
    #     I also added a usage message
    #if options.type in (None,"test_RE"):
        #run_test_RE(options.numreplica,20)   #sample test for Replica Exchange with localhost
    if options.type != None and options.type in ("REMD"):
        run_REMDg(options.configfile) 
    else:
        print "Usage : \n python " + sys.argv[0] + " --type=<REMD> --configfile=<configfile> \n"
        print "Example: \n python " + sys.argv[0] + " --type=REMD --configfile=re_manager_v1.conf"
        sys.exit(1)      
        
    #print "REMDgManager Total Runtime: " + str(time.time()-start) + " s"
    
