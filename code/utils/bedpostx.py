# FDT utility functions for running bedpostx in fsl
# inputs: layout --> BIDSLayout object loaded from study directory
#         entry  --> structure with all the user defined inputs 

import os, sys, subprocess, multiprocessing, glob, getopt, bids, json, re, warnings
from subprocess import PIPE
import pandas as pd

def worker(name,cmdfile):
    """Executes the bash script"""
    process = subprocess.Popen(cmdfile.split(), stdout=PIPE, stderr=PIPE, universal_newlines=True)
    output, error = process.communicate()
    print(error)
    print('Worker: ' + name + ' finished')
    return
               
# run tractography on preprocessed dwi
def run(layout,entry):

  # using the distortion corrected dti images, we will compute tractograpy
  itr=0; s=', ';
  nfiles = len(layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi'))
  jobs=[];
                                             
  dwi=layout.get(subject=entry.pid, scope='derivatives', extension='nii.gz', suffix='dwi');
  preproc_img = dwi.path
  spath = preproc_img.replace("dwi.nii.gz","")
  
  if not os.path.exists(entry.outputs + '/FDT/' + spath + '.bedpostX'):                                        
      bval = layout.get_bval(dwi.path)
      bvec = layout.get_bvec(dwi.path)
      mask = preproc_img.replace('.nii.gz','_brain-mask.nii.gz')

      print("Running bedpostx: " + preproc_img)

      # write bash script for execution
      original_stdout = sys.stdout # Save a reference to the original standard output
      sys.stdout.flush()

      with open(entry.wd + '/cmd_bedpost_dwi.sh', 'w') as fid:
        sys.stdout = fid # Change the standard output to the file we created.

        print('#!/usr/bin/bash')
        print('mkdir -p ' + entry.wd + '/bedpostx_dwi')
        print('cd ' + entry.wd)
        print('imcp ' + preproc_img + ' bedpostx_dwi/data.nii.gz')
        print('imcp ' + mask + ' bedpostx_dwi/nodif_brain_mask.nii.gz')    
        print('cp ' + bval + ' bedpostx_dwi/bvals')                                      
        print('cp ' + bvec + ' bedpostx_dwi/bvecs')   
        
        # add commands here...
                                             
        print("bedpostx bedpostx_dwi")

        #move outputs to derivative folder
        spath = preproc_img.replace("dwi.nii.gz","")
        print('mv bedpostx_dwi.bedpostX ' + entry.outputs + '/FDT/' + spath + '.bedpostX' )

        sys.stdout = original_stdout # Reset the standard output to its original value

      # change permissions to make sure file is executable 
      os.chmod(entry.wd + '/cmd_bedpost_dwi.sh', 0o774)

      # run script
      cmd = 'bash ' + entry.wd + '/cmd_bedpost_dwi.sh'
      name = 'bedpost' + str(itr)
      p = multiprocessing.Process(target=worker, args=(name,cmd))
      jobs.append(p)
      p.start()

      print(p)
  for job in jobs:
    job.join()  #wait for all eddy commands to finish

  ## end run
