# FDT utility functions generate bids layout
# inputs: layout --> BIDSLayout object loaded from study directory
#         entry  --> structure with all the user defined inputs 
import os, sys, subprocess, multiprocessing, glob, getopt, bids, json, re, warnings
from subprocess import PIPE
import pandas as pd

# ------------------------------------------------------------------------------
#  Parse Bids inputs for this script
# ------------------------------------------------------------------------------
def data(entry):

    bids.config.set_option('extension_initial_dot', True)

    layout = bids.BIDSLayout(entry.inputs, derivatives=False, absolute_paths=True)

    if not os.path.exists(entry.outputs + '/FDT') or os.path.exists(entry.outputs + '/FDT/' + 'dataset_description.json'):
      os.makedirs(entry.outputs,exist_ok=True)
      os.makedirs(entry.outputs + '/FDT', exist_ok=True)

      # make dataset_description file...

      lines = {
        'Name': 'FSL Diffusion Toolbox Minimal Preprocessing',
        "BIDSVersion": "1.1.1",
        "PipelineDescription": { 
              "Name": "FSL Diffusion Toolbox",
              "Version": "0.0.3",
              "CodeURL": "https://github.com/Intermountainneuroimaging/dti-preproc"
              },
        "CodeURL": "https://github.com/Intermountainneuroimaging/dti-preproc",
        "HowToAcknowledge": "Please cite all relevant works for FSL tools: topup, eddy, dtifit and python tools: pybids ( https://doi.org/10.21105/joss.01294,  https://doi.org/10.21105/joss.01294)"}

      with open(entry.outputs + '/FDT/' + 'dataset_description.json', 'w') as outfile:
          json.dump(lines, outfile, indent=2)

    return layout
