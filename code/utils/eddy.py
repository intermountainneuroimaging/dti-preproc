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


# (Option 1) run eddy on each input scan seperately (no multi-scan concatination)
def run_eddy_opt1(layout,entry):
    
    itr=0;
    nfiles = len(layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi'))
    jobs=[];

    if entry.use_repol:
      use_repol="--repol"  # option in eddy to replace outliers with gaussian estimate
    else:
      use_repol=""

    for dwi in layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi'):
        
        img = dwi.path
        
        # output filename...
        ent = layout.parse_file_entities(img)

        # Define the pattern to build out of the components passed in the dictionary
        pattern = "sub-{subject}/[ses-{session}/]sub-{subject}[_ses-{session}][_task-{task}][_acq-{acquisition}][_rec-{reconstruction}][_run-{run}][_echo-{echo}][_dir-{direction}][_space-{space}][_desc-{desc}]_{suffix}.nii.gz",

        # Add additional info to output file entities
        ent['space'] = 'native'
        ent['desc'] = 'preproc'

        outfile = layout.build_path(ent, pattern, validate=False, absolute_paths=False)
        outbval = outfile.replace('nii.gz','bval')
        outbvec = outfile.replace('nii.gz','bvec')
        outmask = outfile.replace('.nii.gz','_brain-mask.nii.gz')
        outqc   = outfile.replace('.nii.gz','.qc')
        
        if os.path.exists(entry.wd + '/eddy_dwi_' + str(itr) + '/eddy_unwarped_images.nii.gz'):
            itr=itr+1
            print("Eddy output exists...skipping: " + outfile)
            continue
            print(" ")

        bval = layout.get_bval(dwi.path)
        bvec = layout.get_bvec(dwi.path)
        s=', '
        print('Using: ' + img)
        print('Using: ' + bval)
        print('Using: ' + bvec)

        print("corrected image: " + outfile)

        # write bash script for execution
        original_stdout = sys.stdout # Save a reference to the original standard output
        sys.stdout.flush()

        with open(entry.wd + '/cmd_eddy_opt1_iter0' + str(itr) + '.sh', 'w') as fid:
          sys.stdout = fid # Change the standard output to the file we created.

          print('#!/usr/bin/bash')
          print('mkdir -p ' + entry.wd + '/eddy_dwi_' + str(itr))
          print('cd ' + entry.wd + '/eddy_dwi_' + str(itr))
          
          # add commands here...
          print('cp '+bval+' bval')
          print('cp '+bvec+' bvec')
          print('bvalfile=' + bval)
          print('n="$(grep -n "0" $bvalfile | cut -f1 -d":" )"')
          print('frame=`echo $n | cut -d" " -f1`')
          print('vol="$(($frame-1))"')
          print('echo "Using dwi volume : " $vol " for reference"')
          print('\n')
          print('fslroi ' + img + ' b0 $vol 1')
          topup_img = '../topup/topup_b0'
          acqparams = '../acqparams.txt'

          if 'AP' in img:
            inindex=1  # dwi images collected with acqparameters in row 1
          elif 'PA' in img: 
            inindex=2  # dwi images collected with acqparameters in row 1
          else:
            raise CustomError("Unable to determine if dwi image collected A->P or P->A")
          print('fslroi ' + img + ' b0 0 1') 
          print("""applytopup --imain=b0 \
                 --topup=""" + topup_img + """ \
                 --datain=""" + acqparams + """ \
                 --inindex=""" + str(inindex) + """ \
                 --method=jac \
                 --out=ref""")

          print('bet ref ref_brain -m -f 0.2')

          print('imglen=`fslval ' + img + ' dim4`')

          print('for c in $(seq 1 $imglen); do echo ' + str(inindex) + ' ; done > index.txt')

          print("""eddy_openmp --imain=""" + img + """ \
            --mask=ref_brain_mask \
            --index=index.txt \
            --acqp=""" + acqparams + """ \
            --bvecs=""" + bvec + """ \
            --bvals="""+ bval + """ \
            --fwhm=0 \
            --topup=""" + topup_img + """ \
            --flm=quadratic \
            --out=eddy_unwarped_images \
            """ + use_repol + """ \
            --cnr_maps   \
            --data_is_shelled""")

          if entry.eddy_QC == True:
            print("""eddy_quad eddy_unwarped_images  \
              -idx index.txt \
              -par """ + acqparams + """ \
              -m ref_brain_mask \
              -b """ + bval + """ \
              -g """ + bvec + """ \
              -f """ + topup_img + '_fout')

          print('mkdir -p $(dirname "' + entry.outputs + '/FDT/' + outfile + '")' )
          print('cp -rp eddy_unwarped_images.qc/ ' + entry.outputs + '/FDT/' + outqc)

          sys.stdout = original_stdout # Reset the standard output to its original value

        # change permissions to make sure file is executable 
        os.chmod(entry.wd + '/cmd_eddy_opt1_iter0' + str(itr) + '.sh', 0o774)

        # run script
        cmd = 'bash ' + entry.wd + '/cmd_eddy_opt1_iter0' + str(itr) + '.sh'
        name = 'eddy' + str(itr)
        p = multiprocessing.Process(target=worker, args=(name,cmd))
        jobs.append(p)
        p.start()

        itr = itr+1
        print(p)
    for job in jobs:
      job.join()  #wait for all eddy commands to finish

    # concatinate scans after all runs complete...
    concat_eddy_results(layout,entry)

    ## end run_eddy_opt1

def concat_eddy_results(layout,entry):

  itr=0; s=', ';
  nfiles = len(layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi'))

  # output filename...
  dwi = layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi')[0]
  ent = layout.parse_file_entities(dwi.path)

  # Define the pattern to build out of the components passed in the dictionary
  pattern = "sub-{subject}/[ses-{session}/]sub-{subject}[_ses-{session}][_task-{task}][_acq-{acquisition}][_rec-{reconstruction}][_run-{run}][_echo-{echo}][_dir-{direction}][_space-{space}][_desc-{desc}]_{suffix}.nii.gz",

  # Add additional info to output file entities
  ent['space'] = 'native'
  ent['desc'] = 'preproc'
  ent['direction'] = []  # combined set of dwi

  outfile = layout.build_path(ent, pattern, validate=False, absolute_paths=False)
  outbval = outfile.replace('nii.gz','bval')
  outbvec = outfile.replace('nii.gz','bvec')
  outmask = outfile.replace('.nii.gz','_brain-mask.nii.gz')
  outref  = outfile.replace('.nii.gz','ref.nii.gz')
  outqc   = outfile.replace('.nii.gz','.qc')
  outbase   = outfile.replace('_dwi.nii.gz','')
  
  if not os.path.exists(entry.outputs + '/FDT/' + outref):

    print('Concatenating dwi images...')
  
    # get links to all input data...
    imglist=[]; bvallist=[]; bveclist=[]; indexlist=[]; masklist=[]; 
   
    # join bvals and bvecs from all scans
    cmd = ''
    cmd += 'mkdir -p ' + entry.wd + '/dwi_concat ; \n '
    cmd += 'cd ' + entry.wd + '/dwi_concat ; \n '
    cc=0

    for d in sorted(glob.glob(entry.wd + '/eddy_dwi*[0-9]*')):
      bvallist.append(d+'/bval')
      bveclist.append(d+'/eddy_unwarped_images.eddy_rotated_bvecs')
      imglist.append(d+'/eddy_unwarped_images.nii.gz')
      indexlist.append(d+'/index.txt')
      masklist.append(d+'/ref_brain_mask.nii.gz')
    s=" "
    cmd += 'touch bvecs; touch bvals; touch index.txt \n '
    cmd += 'paste -d " " '+s.join(bvallist)+' > bvals; \n '  # use to horizontally concatenate bval files
    cmd += 'paste -d " " '+s.join(bveclist)+' > bvecs; \n '  # use to horizontally concatenate bvec files
    cmd += 'for i in "'+s.join(indexlist)+'"; do cat $i ; done > index.txt; \n '

    # merge all raw images...
    cmd += 'fslmerge -t dataout '+s.join(imglist)+' ; \n '

    # get mask, order doesnt matter...
    cmd += 'images=$(ls ../eddy_dwi_?/ref_brain_mask.nii.gz); \n '
    cmd += 'nscans=$(ls -1 ../eddy_dwi_?/ref_brain_mask.nii.gz | wc -l); \n'


    # do some fancy bash tricks to get a single line fslmaths command to get average mask
    cmd += 'echo "fslmaths ">tmp ; for i in $images; do echo "$i -add"; done >> tmp ; \n'  # print all individual masks to command
    cmd += "sed -i '$ s/.....$//' tmp; \n"                                                 # remove extra -add tag
    cmd += 'echo "-div $nscans avg_brain_mask" >> tmp; \n'                                 # add rest of command
    cmd += "a=`sed ':a;N;$!ba;s/" + "\\" + "n" + "/ /g' tmp`; \n"                          # make command single line
    cmd += "$a ; \n"                                                                       # run command
    cmd += 'fslmaths avg_brain_mask -thr 0.5 -bin brain_mask ; \n '
    cmd += 'rm avg_brain_mask.nii.gz ; \n'

    # grab the first reference image
    cmd += 'cp ../eddy_dwi_0/ref_brain.nii.gz ref_brain.nii.gz ; \n'

    # save outputs...
    cmd += 'mkdir -p $(dirname "' + entry.outputs + '/FDT/' + outfile + '") \n'
    cmd += '${FSLDIR}/bin/imcp dataout.nii.gz ' + entry.outputs + '/FDT/' + outfile + ' \n'
    cmd += 'cp -p bvals ' + entry.outputs + '/FDT/' + outbval + ' \n'
    cmd += 'cp -p bvecs ' + entry.outputs + '/FDT/' + outbvec + ' \n'
    cmd += 'cp -p brain_mask.nii.gz ' + entry.outputs + '/FDT/' + outmask + ' \n'
    cmd += 'cp -p ref_brain.nii.gz ' + entry.outputs + '/FDT/' + outref + ' \n'

    original_stdout = sys.stdout # Save a reference to the original standard output
    sys.stdout.flush()

    with open(entry.wd + '/cmd_eddy1_concat.sh', 'w') as fid:
      sys.stdout = fid # Change the standard output to the file we created.

      print('#!/usr/bin/bash')
      print(cmd)

      sys.stdout = original_stdout # Reset the standard output to its original value

    # change permissions to make sure file is executable 
    os.chmod(entry.wd + '/cmd_eddy1_concat.sh', 0o774)

    # run script
    cmd = "bash " + entry.wd + "/cmd_eddy1_concat.sh"
    name = 'eddy'
    p = multiprocessing.Process(target=worker, args=(name,cmd))
    p.start()

    generate_confounds_file(entry,entry.outputs + '/FDT/' + outbase)

  #END concat_eddy1_results

# (Option 2) run eddy on concatenated dwi scans (consistent to MRN, HCP pipelines (pairs))
def run_concat_inputs(layout,entry):
  # get links to all input data...
  if not os.path.exists(entry.wd + '/eddy_dwi_concat/brain_mask.nii.gz'):
    imglist=[]; bvallist=[]; bveclsit=[];

    # join bvals and bvecs from all scans
    cmd = ''
    cmd += 'mkdir -p ' + entry.wd + '/eddy_dwi_concat ; \n '
    cmd += 'cd ' + entry.wd + '/eddy_dwi_concat ; \n '
    cc=0
    for dwi in layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi'):
      img = dwi.path
      print(img)
      cmd += 'ln -sf ' + dwi.path + ' dwi_' + str(cc) + '.nii.gz ; \n'
      cmd += 'ln -sf ' + layout.get_bval(dwi.path) + ' bval_' + str(cc) + ' ; \n' 
      cmd += 'ln -sf ' + layout.get_bvec(dwi.path) + ' bvec_' + str(cc) + ' ; \n' 

      topup_img = '../topup/topup_b0'
      acqparams = '../acqparams.txt'

      if 'AP' in img:
        inindex=1  # dwi images collected with acqparameters in row 1
      elif 'PA' in img: 
        inindex=2  # dwi images collected with acqparameters in row 1
      else:
        raise CustomError("Unable to determine if dwi image collected A->P or P->A")

      cmd += 'imglen=`fslval ' + img + ' dim4` ; \n'
      cmd += 'for c in $(seq 1 $imglen); do echo ' + str(inindex) + ' ; done > index_' + str(cc) + '.txt ; \n'

      cmd += 'fslroi ' + img + ' b0 0 1 ; \n'
      cmd += 'applytopup --imain=b0 --topup=' + topup_img + ' --datain=' + acqparams + ' --inindex=' + str(inindex) + ' --method=jac --out=ref ; \n'

      cmd += 'bet ref ref_brain -m -f 0.2 ; \n'
      cmd += 'mv ref_brain_mask.nii.gz ref_brain_mask_' + str(cc) + '.nii.gz ; \n'
      cmd += 'mv ref_brain.nii.gz ref_brain_' + str(cc) + '.nii.gz ; \n'

      cc=cc+1

    cmd += 'touch bvecs; touch bvals; touch index_all.txt \n '
    cmd += 'paste -d " " $(ls bval_?) > bvals; \n '  # use to horizontally concatenate bval files
    cmd += 'paste -d " " $(ls bvec_?) > bvecs; \n '  # use to horizontally concatenate bvec files
    cmd += 'for i in $(ls index_?.txt); do cat $i ; done > index_all.txt; \n '

    # merge all raw images...
    cmd += 'images=$(ls dwi_?.nii.gz); \n '
    cmd += 'fslmerge -t data $images ; \n '

    # get mask...
    cmd += 'images=$(ls ref_brain_mask_?.nii.gz); \n '
    cmd += 'nscans=$(ls -1 ref_brain_mask_?.nii.gz | wc -l); \n'


    # do some fancy bash tricks to get a single line fslmaths command to get average mask
    cmd += 'echo "fslmaths ">tmp ; for i in $images; do echo "$i -add"; done >> tmp ; \n'  # print all individual masks to command
    cmd += "sed -i '$ s/.....$//' tmp; \n"                                                 # remove extra -add tag
    cmd += 'echo "-div $nscans avg_brain_mask" >> tmp; \n'                                 # add rest of command
    cmd += "a=`sed ':a;N;$!ba;s/" + "\\" + "n" + "/ /g' tmp`; \n"                          # make command single line
    cmd += "$a ; \n"                                                                       # run command
    cmd += 'fslmaths avg_brain_mask -thr 0.5 -bin brain_mask ; \n '
    cmd += 'rm avg_brain_mask.nii.gz ; \n'

    # write bash script for execution
    original_stdout = sys.stdout # Save a reference to the original standard output
    sys.stdout.flush()

    with open(entry.wd + '/cmd_eddy2_concat.sh', 'w') as fid:
      sys.stdout = fid # Change the standard output to the file we created.

      print('#!/usr/bin/bash')
      print(cmd)

      sys.stdout = original_stdout # Reset the standard output to its original value

    # change permissions to make sure file is executable 
    os.chmod(entry.wd + '/cmd_eddy2_concat.sh', 0o774)

    # run script
    cmd = "bash " + entry.wd + "/cmd_eddy2_concat.sh"
    name = 'eddy'
    p = multiprocessing.Process(target=worker, args=(name,cmd))
    p.start()


def run_eddy_opt2(layout,entry):

    itr=0; s=', ';
    nfiles = len(layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi'))

    # output filename...
    dwi = layout.get(subject=entry.pid, extension='nii.gz', suffix='dwi')[0]
    ent = layout.parse_file_entities(dwi.path)

    # Define the pattern to build out of the components passed in the dictionary
    pattern = "sub-{subject}/[ses-{session}/]sub-{subject}[_ses-{session}][_task-{task}][_acq-{acquisition}][_rec-{reconstruction}][_run-{run}][_echo-{echo}][_dir-{direction}][_space-{space}][_desc-{desc}]_{suffix}.nii.gz",

    # Add additional info to output file entities
    ent['space'] = 'native'
    ent['desc'] = 'preproc'
    ent['direction'] = []  # combined set of dwi

    outfile = layout.build_path(ent, pattern, validate=False, absolute_paths=False)
    outbval = outfile.replace('nii.gz','bval')
    outbvec = outfile.replace('nii.gz','bvec')
    outmask = outfile.replace('.nii.gz','_brain-mask.nii.gz')
    outqc   = outfile.replace('.nii.gz','.qc')
    outref  = outfile.replace('.nii.gz','ref.nii.gz')
    outbase   = outfile.replace('_dwi.nii.gz','')

    if entry.use_repol:
      use_repol="--repol"  # option in eddy to replace outliers with gaussian estimate
    else:
      use_repol=""
    
    if os.path.exists(entry.outputs + '/FDT/' + outfile):
      print('Eddy output exists...skipping')
    else:
      print('Concatenating dwi images...')

      run_concat_inputs(layout,entry)

      print('Running Eddy...')

      jobs=[];

      # write bash script for execution
      original_stdout = sys.stdout # Save a reference to the original standard output
      sys.stdout.flush()

      with open(entry.wd + '/cmd_eddy_opt2.sh', 'w') as fid:
        sys.stdout = fid # Change the standard output to the file we created.

        print('#!/usr/bin/bash')
        print('cd ' + entry.wd + '/eddy_dwi_concat ; ')

        topup_img = '../topup/topup_b0'
        acqparams = '../acqparams.txt'

        print("""eddy_openmp --imain=data.nii.gz \
          --mask=brain_mask \
          --index=index_all.txt \
          --acqp=""" + acqparams + """ \
          --bvecs=bvecs \
          --bvals=bvals \
          --fwhm=0 \
          --topup=""" + topup_img + """ \
          --flm=quadratic \
          --out=eddy_unwarped_images \
          """ + use_repol + """ \
          --cnr_maps   \
          --data_is_shelled""")

        if entry.eddy_QC == True:
          print("""eddy_quad eddy_unwarped_images  \
            -idx index_all.txt \
            -par """ + acqparams + """ \
            -m brain_mask \
            -b bvals \
            -g bvecs \
            -f """ + topup_img + '_fout')

        print('mkdir -p $(dirname "' + entry.outputs + '/FDT/' + outfile + '")' )
        print('${FSLDIR}/bin/imcp eddy_unwarped_images.nii.gz ' + entry.outputs + '/FDT/' + outfile)
        print('cp -p bvals ' + entry.outputs + '/FDT/' + outbval)
        print('cp -p eddy_unwarped_images.eddy_rotated_bvecs ' + entry.outputs + '/FDT/' + outbvec)
        print('cp -p brain_mask.nii.gz ' + entry.outputs + '/FDT/' + outmask)
        print('cp -p ref_brain_0.nii.gz ' + entry.outputs + '/FDT/' + outref)
        print('cp -rp eddy_unwarped_images.qc/ ' + entry.outputs + '/FDT/' + outqc)

        sys.stdout = original_stdout # Reset the standard output to its original value

      # change permissions to make sure file is executable 
      os.chmod(entry.wd + '/cmd_eddy_opt2.sh', 0o774)

      # run script
      cmd = "bash " + entry.wd + "/cmd_eddy_opt2.sh"
      name = 'eddy'
      p = multiprocessing.Process(target=worker, args=(name,cmd))
      jobs.append(p)
      p.start()

      print(p)

      for job in jobs:
        job.join()  #wait for all eddy commands to finish

      generate_confounds_file(entry,entry.outputs + '/FDT/' + outbase)


    ## end run_eddy_opt2


def generate_confounds_file(entry,outbase):

  # after running fsl outliers - put all coundounds into one file
  
  df=pd.DataFrame()

  # read in RMS motion data from eddy (detailed description on eddy user guide)

  for f in sorted(glob.glob(entry.wd + '/eddy_dwi*/eddy_unwarped_images.eddy_movement_rms')):
    d=pd.read_csv(f,sep="\s+")  
    colnames=["eddy_movement_rms_abs","eddy_movement_rms_disp"]
    d.columns = colnames

    df = pd.concat([df,d],axis=0)

  # output a single confounds file
  df.to_csv(outbase + "_confounds.tsv",sep="\t")

  df=pd.DataFrame()
  for f in sorted(glob.glob(entry.wd + '/eddy_dwi*/eddy_unwarped_images.eddy_outlier_report')):
    d2=pd.read_csv(f,header=None,sep="\t")  
    d1=pd.DataFrame([f])
    d = pd.concat([d1,d2],axis=0)
    df = pd.concat([df,d],axis=0)

  # output a single outlier file
  df.to_csv(outbase + "_outlier_log.txt",sep="\t",index=False)

  # END GENERATE_CONFOUNDS_FILE
