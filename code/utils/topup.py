# FDT utility functions for running topup in fsl
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

def run(layout,entry):
        
    # check if blip-up blip-down aquisition (ie dwi collected in opposing phase encoding directions)
    for dwi in layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi'):
        ent = dwi.get_entities()
        if 'AP' in ent['direction']:
            if 'img1' not in locals():
                img1=dwi.path
                meta=dwi.get_metadata()
        elif 'PA' in ent['direction']:
            if 'img2' not in locals():
                img2=dwi.path

    # if blip-up blip-down is used: pull b0 from both images and merge for topup
    
    print('Applying topup: ')
    cmd='rm ' + entry.wd + '/acqparams.txt ; touch ' + entry.wd + '/acqparams.txt ;'
    if 'img1' in locals():
        cmd += 'fslroi ' + img1 + ' b0_AP 0 1; '  # takes the first volume of dwi image as b0
        cmd += 'echo "0 -1 0 ' + str(meta['TotalReadoutTime']) + '" >> ' + entry.wd + '/acqparams.txt ; ' # acqparameters for A -> P
        
        refimg = 'b0_AP'
        print('AP acquisition Using: ' + img1)

    if 'img2' in locals():
        cmd += 'fslroi ' + img2 + ' b0_PA 0 1; '  # takes the first volume of dwi image as b0  
        cmd += 'echo "0 1 0 ' + str(meta['TotalReadoutTime']) + '" >> ' + entry.wd + '/acqparams.txt ; ' # acqparameters for P -> A

        refimg = 'b0_AP'
        print('PA acquisition Using: ' + img2)

    if ('img1' in locals()) and ('img2' in locals()):
        cmd += 'fslmerge -t b0_APPA b0_AP b0_PA'
        refimg = 'b0_APPA'

    
    # write bash script for execution
    original_stdout = sys.stdout # Save a reference to the original standard output
    sys.stdout.flush()
    jid=[]

    with open(entry.wd + '/cmd_topup.sh', 'w') as fid:
      sys.stdout = fid # Change the standard output to the file we created.
      
      print('#!/usr/bin/bash')
      print('mkdir -p ' + entry.wd + '/topup')
      print('cd ' + entry.wd + '/topup')
      print(cmd)

      print("""topup --imain=""" + refimg + """ \
        --datain=../acqparams.txt \
        --config=b02b0.cnf \
        --out=topup_b0 \
        --iout=topup_b0_iout \
        --fout=topup_b0_fout  \
        --logout=topup""")

      sys.stdout = original_stdout # Reset the standard output to its original value

    # change permissions to make sure file is executable 
    os.chmod(entry.wd + '/cmd_topup.sh', 0o774)

    # run script
    cmd = "bash " + entry.wd + "/cmd_topup.sh"
    name = "topup"
    p = multiprocessing.Process(target=worker, args=(name,cmd))
    p.start()
    print(p)

    p.join()  # blocks further execution until job is finished

    ## end run_topup
