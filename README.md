# DataCleanser_CSV_SF_load
************************************************************
*    Utility to split CSV for migration to Salesforce      *
*    Perform data cleansing also, removing HTML tags if any*
************************************************************

Pre-requisites:
============================================================
1. Install Python (3.6 or above)
   https://www.python.org/downloads/

2. Install the following packages.
   -Open windows command prompt, enter the following commands:
     pip install pandas
     pip install random *this might in most probable cases, be pre-installed.

3. Configure the list of tags that needs to be removed from the dataframe.
   - file : .../config/parameters.txt
     if it is a staight forward replace - enter them as is eg., <body>, <td>

     if the tag contains dynamic content, you'd need to append it with " *[^<]+?* " where * represents the start and
     end pattern. eg., <tbody [^<]+?> => removes tag starting with "<tbody" and ending with ">".

Execution steps:
============================================================
1. Save the files sfb_load.py, write_log to any local directory.
   review (and update if required) the following variables in the script sfb_load.py
     input_dir = r"C:\sfb_load\input\\"
     output_dir = r"C:\sfb_load\output\\" #Folder location where the split file will be stored. There will be 2 files in total
     log_path = r"C:\sfb_load\log\\"

2. Once step 3 is OK, place all your input CSV files that needs to be split in the directory specified as "input_dir"

3. On command prompt, navigate to the folder where sfb_load.py is saved.
   execute the command "python sfb_load.py"

4. The detailed log could be found in the "log_path" directory.
