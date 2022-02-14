#! usr/bin/env python

# ## PIPELINE: fdtpipeline.py
# ## USAGE: python3 fdtpipeline --in=<inputs> --out=<outputs> [OPTIONS]
#    * requires python3 and FSL (calls FSL via python subprocess)
#
# ## Author(s)
#
# * Amy K. Hegarty, Intermountain Neuroimaging Consortium, University of Colorado Boulder
# * University of Colorado Boulder
#
# ## Product
#
# FSL Pipelines
#
# ## License
#
# <!-- References -->
# [FSL]: https://fsl.fmrib.ox.ac.uk/fsl/fslwiki
# [pybids]: Yarkoni et al., (2019). PyBIDS: Python tools for BIDS datasets. Journal of Open Source Software, 4(40), 1294, https://doi.org/10.21105/joss.01294
#           Yarkoni, Tal, Markiewicz, Christopher J., de la Vega, Alejandro, Gorgolewski, Krzysztof J., Halchenko, Yaroslav O., Salo, Taylor, ? Blair, Ross. (2019, August 8). bids-standard/pybids: 0.9.3 (Version 0.9.3). Zenodo. http://doi.org/10.5281/zenodo.3363985
#

import os
import sys
import subprocess
import multiprocessing
import glob
import getopt
import bids
import json
import subprocess
from subprocess import PIPE
import re
import warnings
from utils import eddy, dtifit, topup, bedpostx, report, custombids


# ------------------------------------------------------------------------------
#  Show usage information for this script
# ------------------------------------------------------------------------------

def print_help():
  print("""
    Diffusion Preprocessing Pipeline
        Usage: """ + """ --in=<bids-inputs> --out=<outputs> [OPTIONS]
        OPTIONS
          --help                      show this usage information and exit
          --participant-label=        participant name for processing (pass only 1)
          --work-dir=                 (Default: /scratch) directory path for working 
                                        directory
          --clean-work-dir=           (Default: TRUE) clean working directory 
          --concat-before-preproc=    (Default: FALSE) boolean to select if input images
                                        should be concatenated before preprocessing
          --run-qc=                   (Default: TRUE) boolean to run automated quality 
                                        control for eddy corrected images
          --use-repol                 add flag correct outliers in eddy (see more: fsl/eddy 
                                        user guide)
          --ignore-preproc            add flag to ignore preprocessing steps, and skip to 
                                        running tensor-fit or bedpostx. Only use if preprocessing
                                        is already completed (e.g. qsiprep outputs)  
          --run-tensor-fit            add flag to run tensor-fit processing on 
                                        preprocessed images
          --run-bedpostx              add flag to run bedpostx tractography processing on 
                                        preprocessed images (default settings used for analysis)
    ** OpenMP used for parellelized execution of eddy. Multiple cores (CPUs) 
       are recommended (4 cpus for each dwi scan).
       
    ** see github repository for more information and to report issues: 
       https://github.com/amyhegarty/docker-fsl-fdt.git
        
        """)

# ------------------------------------------------------------------------------
#  Parse arguements for this script
# ------------------------------------------------------------------------------

def parse_arguments(argv):

    #intialize arguements
    print("\nParsing User Inputs...")
    cat = False
    qc = True
    cleandir = False
    runfit = False
    runbedpostx = False
    use_repol = False
    ignore_preproc = False


    try:
      opts, args = getopt.getopt(argv,"hi:o:",["in=","out=","help","participant-label=","work-dir=","clean-work-dir=","concat-before-preproc=","run-qc=","use-repol","ignore-preproc","run-tensor-fit","run-bedpostx"])
    except getopt.GetoptError:
      print_help()
      sys.exit(2)
    for opt, arg in opts:
      if opt in ("-h", "--help"):
         print_help()
         sys.exit()
      elif opt in ("-i", "--in"):
         inputs = arg
         if not os.path.exists(inputs):
           raise Exception("BIDS directory does not exist")
      elif opt in ("-o", "--out"):
         outputs = arg
      elif opt in ("--participant-label"):
         pid = arg
      elif opt in ("--work-dir"):
         wd = arg
      elif opt in ("--clean-work-dir"):
         cleandir = arg
      elif opt in ("--concat-before-preproc"):
         cat = arg
         if cat in ("TRUE", "True", "true"):
            cat = True
         elif cat in ("FALSE", "False", "false"):
            cat = False
         else:
            raise Exception("Error: --concat-preproc= [TRUE / FALSE]")
      elif opt in ("--run-qc"):
         qc = arg
         if qc in ("TRUE", "True", "true"):
            qc = True
         elif qc in ("FALSE", "False", "false"):
            qc = False
         else:
            raise Exception("Error: --run-qc= [TRUE / FALSE]")
      elif opt in ("--run-tensor-fit"):
        runfit = True
      elif opt in ("--run-bedpostx"):
        runbedpostx = True       
      elif opt in ("--use-repol"):
        use_repol = True  
      elif opt in ("--ignore-preproc"):
        ignore_preproc = True  
                                        
    if 'inputs' not in locals():
      print_help()
      raise Exception("Missing required argument --in=")
      sys.exit()
    if 'outputs' not in locals():
      print_help()
      raise Exception("Missing required argument --out=")
      sys.exit()
    if 'pid' not in locals():
      print_help()
      raise Exception("Missing required argument --participant-label=")
      sys.exit()
    if 'wd' not in locals():
      wd=outputs+'/scratch/sub-'+pid
      
    print('Input Bids directory:\t', inputs)
    print('Derivatives path:\t', outputs)
    print('Participant:\t\t', str(pid))

    class args:
      def __init__(self, wd, inputs, outputs, pid, cat, qc, cleandir, runfit, runbedpostx,use_repol,ignore_preproc):
        self.wd = wd
        self.inputs = inputs
        self.outputs = outputs
        self.pid = pid
        self.concat= cat
        self.eddy_QC=qc
        self.cleandir=cleandir
        self.rundtifit=runfit
        self.runbedpostx=runbedpostx
        self.use_repol=use_repol
        self.ignore_preproc=ignore_preproc

    entry = args(wd, inputs, outputs, pid, cat, qc, cleandir, runfit, runbedpostx, use_repol,ignore_preproc)

    return entry

# ------------------------------------------------------------------------------
#  Main Pipeline Starts Here...
# ------------------------------------------------------------------------------
 
def main(argv):

    # get user entry
    entry = parse_arguments(argv)

    os.makedirs(entry.wd, exist_ok=True)
    logdir = entry.wd + '/logs'
    os.makedirs(logdir, exist_ok=True)

    # get participant bids path:
    db = custombids.data(entry)

    if not entry.ignore_preproc:
      # pipeline: (1) topup, (2) eddy, (3) dtifit
      if not os.path.exists(entry.wd + '/topup/topup_b0_iout.nii.gz'):
          topup.run(db,entry)
      
      # two run options: 
      if entry.concat == False:
        # (1) distortion and eddy correct each aquisition seperately
        eddy.run_eddy_opt1(db,entry)

      else:
        # (2) concatenate all aquisitions before preprocessing
        eddy.run_eddy_opt2(db,entry)

    db.add_derivatives(entry.outputs + '/FDT')

    if entry.rundtifit == True:
      dtifit.run(db,entry)
    
    if entry.runbedpostx == True:
      bedpost.run(db,entry)

    # clean-up
    report.cleanup(entry)
    

if __name__ == "__main__":
    main(sys.argv[1:])
