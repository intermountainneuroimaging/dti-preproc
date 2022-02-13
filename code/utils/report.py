# FDT utility functions for running FDT report
# inputs: layout --> BIDSLayout object loaded from study directory
#         entry  --> structure with all the user defined inputs 

import os, sys, subprocess, multiprocessing, glob, getopt, bids, json, re, warnings
from subprocess import PIPE
import pandas as pd

# add report functions here...

# Steps:
# image of dwi b0 scan?
# image of cnr ?
# image of FA ? -- if run?

# workflow diagram
# log progress of pipeline...(full logs attached?)
def run(entry):
  # compile reports
  print("TO DO...")

                                              
def cleanup(entry):

  jobs=[];
  if entry.cleandir == True:
    # write bash script for execution
    original_stdout = sys.stdout # Save a reference to the original standard output
    sys.stdout.flush()

    with open(entry.wd + '/cmd_cleanup.sh', 'w') as fid:
      sys.stdout = fid # Change the standard output to the file we created.

      print('#!/usr/bin/bash')

      # add commands here...
      print('rm -Rf ' + entry.wd)

      sys.stdout = original_stdout # Reset the standard output to its original value

    # change permissions to make sure file is executable 
    os.chmod(entry.wd + '/cmd_cleanup.sh', 0o774)

    # run script
    cmd = 'bash ' + entry.wd + '/cmd_cleanup.sh'
    name = 'cleanup'
    p = multiprocessing.Process(target=worker, args=(name,cmd))
    jobs.append(p)
    p.start()

    print(p)

    for job in jobs:
      job.join()  #wait for all eddy commands to finish

  ## end run_cleanup
