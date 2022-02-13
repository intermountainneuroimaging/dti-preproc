# FDT utility functions for running eddy in fsl
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
                                               
# run tensor fitting on preprocessed image
def run(layout,entry):

  # using the distortion corrected dti images, we will compute tensor values
  itr=0; s=', ';
  nfiles = len(layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi'))
  jobs=[];

  for dwi in layout.get(subject=entry.pid, scope='derivatives', extension='nii.gz', suffix='dwi'):
  
      preproc_img = dwi.path
      FAfile = preproc_img.replace("dwi.nii.gz","dwi_FA.nii.gz")
    
      if os.path.exists(entry.outputs + '/FDT/' + FAfile):
        continue

      preproc_img = dwi.path
      bval = layout.get_bval(dwi.path)
      bvec = layout.get_bvec(dwi.path)
      mask = preproc_img.replace('.nii.gz','_brain-mask.nii.gz')

      print("Running dtifit: " + preproc_img)

      # write bash script for execution
      original_stdout = sys.stdout # Save a reference to the original standard output
      sys.stdout.flush()

      with open(entry.wd + '/cmd_tensor_dwi_' + str(itr) + '.sh', 'w') as fid:
        sys.stdout = fid # Change the standard output to the file we created.

        print('#!/usr/bin/bash')
        print('mkdir -p ' + entry.wd + '/tensor_dwi_' + str(itr))
        print('cd ' + entry.wd + '/tensor_dwi_' + str(itr))
        # add commands here...
        print("""dtifit --data=""" + preproc_img + """ \
          --mask=""" + mask + """ \
          --bvecs=""" + bvec + """ \
          --bvals="""+ bval + """ \
          --out=dwi """)

        #move outputs to derivative folder
        spath = preproc_img.replace("dwi.nii.gz","")
        print('for i in *.nii.gz; do ${FSLDIR}/bin/imcp $i ' + spath.replace("preproc","dtifit") + '$i ; done')

        sys.stdout = original_stdout # Reset the standard output to its original value

      # change permissions to make sure file is executable 
      os.chmod(entry.wd + '/cmd_tensor_dwi_' + str(itr) + '.sh', 0o774)

      # run script
      cmd = 'bash ' + entry.wd + '/cmd_tensor_dwi_' + str(itr) + '.sh'
      name = 'dtifit' + str(itr)
      p = multiprocessing.Process(target=worker, args=(name,cmd))
      jobs.append(p)
      p.start()

      itr = itr+1
      print(p)
  for job in jobs:
    job.join()  #wait for all eddy commands to finish

  ## end run
