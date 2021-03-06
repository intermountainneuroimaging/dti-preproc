# FMRIB Software Library Diffusion Toolbox: Preprocessing Pipeline

_docker-fsl-fdt_ is a command-line program that uses FMRIB Software Library (FSL) diffusion toolbox for diffusion weighted image preprocessing. Pipeline is built on BIDS compatibility, therefore all inputs and outputs must be in BIDS format (v 1.1.0)

Contents:
  - [Installation](#installation)
  - [Bids Format](#bids-format)
  - [Command-Line Arguments](#command-line-arguments)
  - [Examples](#docker-examples)
    - [Docker](#docker)
    - [Singularity](#singularity)
- [Known Issues](#known-issues)

# Installation

Use the _fsl-fdt_ Docker image (recommended):

```shell
docker run --rm amhe4269/fsl-fdt:0.0.2 --help
```

Use the _fsl-fdt_ Singularity image (recommended):

```shell
singularity pull fsl-fdt docker://amhe4269/fsl-fdt:0.0.2
singularity run amhe4269/fsl-fdt:0.0.2 --help
```
# BIDS format
The fsl-fdt workflow takes advantage of the BIDS naming convention and supporting metadata. The input data must be in a valid BIDS format, and include at least one dwi image with accompanying bval and bvec files. Metadata including readoutime must be including in a json sidecar file for each dwi image. See [dcm2niix](https://github.com/rordenlab/dcm2niix) and [BIDS Validator](https://bids-standard.github.io/bids-validator/) for more details. 

# Command-Line Arguments

```
usage: fsl-fdt [-h] [-i INPUT_PATH] [-o OUTPUT_PATH]
               [--participant-label= ID]
               [--work-dir= SCRATCH_PATH]
               [--clean-work-dir= {TRUE,FALSE}]
               [--concat-before-preproc= {TRUE,FALSE}]
               [--run-qc= {TRUE,FALSE}]
               [--use-repol][--ignore-preproc]
               [--run-tensor-fit][--run-bedpostx]

optional arguments:
  -h, --help                          show this help message and exit
  -i INPUT_PATH, --in= INPUT_PATH        (required) BIDS input directory path
  -o OUTPUT_PATH, --out= OUTPUT_PATH     (required) BIDS output / derivatives directory 
  --participant-label= ID                (required) participant label for processing
  --work-dir= SCARTCH_PATH               select working directory for analysis (DEFAULT: /scratch)
  --clean-work-dir= {TRUE,FALSE}         flag used to define if working directory should be cleared after execution (DEFAULT: TRUE)
  --concat-before-preproc= {TRUE,FALSE}  flag used to select if all dwi images should be concatinated before correction (DEFAULT: FALSE)
  --run-qc= {TRUE,FALSE}                 flag set to include EDDY_QC (DEFAULT: TRUE)
  --use-repol                            add flag correct outliers in eddy (see more: fsl/eddy user guide)
  --ignore-preproc                       add flag to ignore preprocessing steps, and skip to running tensor-fit or bedpostx. Only use if preprocessing is already   
                                          completed (e.g. qsiprep outputs)  
  --run-tensor-fit                       add flag to run tensor-fit processing on preprocessed images
  --run-bedpostx                         add flag to run bedpostx tractography processing on preprocessed images (default settings used for analysis)
  
** OpenMP used for parellelized execution of eddy. Multiple cores (CPUs) are recommended (4 cpus for each dwi scan).

Docker entrypoint: FDTPipeline.py
```
> ### Important
> If running multiple instances of fsl-fdt, you _MUST_ create a unique working directory for each instance to avoid loop contamination.

# Running _fsl-fdt_ using Docker Engine
This pipeline is built with the intented to be used with docker or singularity engines. Compiled in the docker image includes all python packages and FSL version (6.0.3) for the pipeline.
```shell
$ docker run -ti --rm \
    -v path/to/data:/data:ro \
    -v path/to/output:/out \
    amhe4269/fsl-fdt:<latest-version> /data /out --participant-label=[ID] [OPTIONS] 
```

### Docker
Run the Diffusion Toolbox pipleine using docker. We first mount appropriate volumes (external directories) and assign relevant arguments including participant-label.
```shell
$ docker run -ti --rm \
    -v $studydir/BIDS:/data:ro \
    -v $studydir/ANALYSIS:/out \
    -v $studydir/tmp/ds005-workdir:/work \
    amhe4269/fsl-fdt:<latest-version> \
    /data /out/ --participant-label=0001 --work-dir /work --clean-work-dir=FALSE
```

### Singularity
Run the Diffusion Toolbox pipleine using singularity. We first mount appropriate volumes (external directories) and assign relevant arguments including participant-label.
```shell
$ singularity run 
    -bind $studydir/BIDS:/data:ro \
    -bind $studydir/ANALYSIS:/out \
    -bind $studydir/tmp/ds005-workdir:/work \
    amhe4269/fsl-fdt:<latest-version> \
    /data /out/ --participant-label=0001 --work-dir /work --clean-work-dir=FALSE
```

# Known Issues
Working directory must be explicitly defined (in sperate locations) if running multiple instances of fsl-fdt pipeline on the same computational resources.

