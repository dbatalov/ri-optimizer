# ri-optimizer
A script designed to continuously optimize AWS EC2 Reserved Instance (RI) utilization by moving RIs between Availability Zones.

## Details

The general idea of the script is that you have one AWS account or several accounts linked via consolidated billing. The script assumes that all RIs are purchased in one account only, so this could be a limitation, but it should be possible to generalize this. The thing to keep in mind too is that in a consolidated billing situation the RI pricing benefits are based on matching AZ labels (not physical AZs). As a result of this the actual capacity reservation is also distributed across AZs in a fashion that is based on labels. 

Here are more specific instructions:

The two files are `riptimize.py` and `example_main.py`. The first is the Python module that exports the main `riptimize()` function that performs all the tasks and the second is the example driver script that sets things up, imports the `riptimize()` function from the other file, executes it and prints a report as well as saves a copy in CSV format and potentially uploads it to S3.

Here is what you need to do a trial run of the script:

1. Save both files into the same directory.
2. Make sure you are running the latest version of boto, e.g.: `sudo pip install -U boto`
The latest RI modification API is only supported in the latest version
3. Go through `STEP X of X` steps in the `example_main.py` file and make the necessary changes, for the most part you just need to get the credentials of the RI holding account and a couple of accounts that run the on-demand instances
4. You should be good to go: **$**` python example_main.py`

This script can be run from time to time or, say, on an hourly basis: it produces the RI utilization report and performs RI optimizations (redistribution across AZs to maximize RI utilization) if the appropriate flag is set. The output of the script should be fairly self explanatory.
