#!/usr/bin/env python3

# Handles slurm submission of cluster jobs. Will follow the cluster.json settings by default,
# but individual jobs can specify cluster_partition, cluster_time, cluster_memory, or cluster_priority
# under their params directive in order to overwrite the sbatch parameters. 

# Notes:
#    - All slurm command logs are written to the folder .slurm-logs/{job_name} under the working directory
#    - The jobscript will echo the sbatch command to the snakemake log

import os
import shutil
import subprocess
import sys

from snakemake.utils import read_job_properties


workingdir = os.getcwd()

jobscript = sys.argv[1]
jobscript2 = jobscript.replace(".sh", "_new.sh")
job_properties = read_job_properties(jobscript)

# rewrite the jobscript created by snakemake and save it differently
with open(jobscript, "r") as from_file, open(jobscript2, "w") as to_file:
    line = from_file.readline() # read the first line and discard ist
    to_file.write(line.replace("/bin/sh", "/bin/bash"))
    line = from_file.readline() # discard the 2nd line (the comments of snakemake)
    # export path to conda that is needed
    to_file.write("export PATH=/scicore/home/dresch0000/netter0000/miniconda3/condabin:$PATH && \\\n")
    if "params" in job_properties and "modules" in job_properties["params"]:
        for module in job_properties["params"]["modules"]:
            to_file.write(f"module load {module} && \\\n")

    shutil.copyfileobj(from_file, to_file)

submission_params = {
    "workingdir": workingdir,
    "jobscript": jobscript2,
    "cores": job_properties["threads"]
}

submission_param_names = ["partition", "time", "memory", "priority"]
for p in submission_param_names:
    if "params" in job_properties and "cluster_" + p in job_properties["params"]:
        submission_params[p] = job_properties["params"]["cluster_" + p]
    else:
        submission_params[p] = job_properties["cluster"][p]

def file_escape(string):
    return string.replace("/", "_").replace(" ", "_")

if job_properties["type"] == "single":
    submission_params["job_name"] = "snake." + job_properties["rule"] 
    if len(job_properties["wildcards"]) > 0:
        submission_params["job_name"] += "." + ".".join([key + "=" + file_escape(value) for key,value in job_properties["wildcards"].items()])
    submission_params["log_dir"] = os.path.join(workingdir, ".slurm-logs", job_properties["rule"])
elif job_properties["type"] == "group":
    submission_params["job_name"] = "snake." + job_properties["groupid"]
    submission_params["log_dir"] = os.path.join(workingdir, ".slurm-logs", job_properties["groupid"])
else:
    print("Error: slurm-submit.py doesn't support job type {} yet!".format(job_properties["type"]))
    sys.exit(1)

# Make the slurm-logs directory in case it doesn't exist already
# (required for sbatch job submissions to run properly with logging)
os.makedirs(submission_params["log_dir"], exist_ok=True)

submit_string = "sbatch --job-name={job_name} -p {partition} -c {cores}  -t {time} --mem {memory} --qos {priority} --parsable -o {log_dir}/{job_name}.%j.out -e {log_dir}/{job_name}.%j.err {jobscript}".format(**submission_params)

result = subprocess.run(submit_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

sys.stdout.write(result.stdout.decode())

if len(result.stderr) > 0:
    sys.stderr.write(result.stderr.decode())

# Copy jobscript to slurm-logs dir
script_log = os.path.join(submission_params["log_dir"], submission_params["job_name"] + "." + result.stdout.strip().decode() + ".sbatch") 
shutil.copy(submission_params["jobscript"], script_log)
submit_log = os.path.join(submission_params["log_dir"], submission_params["job_name"] + "." + result.stdout.strip().decode() + ".cmd") 
with open(submit_log, "w") as f:
    f.write(submit_string)
