#!/usr/bin/env python3

# Author: Krishna.Shetty@Accenture.com
# Version: 1.0
# 29-Sep-2018.

import pandas as pd
import random as rd
import os
import write_log as log
import datetime
from csv import QUOTE_ALL  # package to add quote for all fields in the final files.

# CONFIGURATION
# File containing all the pre identified HTML tags that needs to be removed before SF loading.
tag_param_for_removal = r"C:\Users\krishna.shetty\PycharmProjects\SFBLoad\config\parameters.txt"

# Directory that contains all source CSV files that needs to be cleansed.
input_dir = r"C:\sfb_load\input\\"

# Directory where the cleansed, transformed files will be generated.
output_dir = r"C:\sfb_load\output\\"  # Folder location where the split file will be stored. 2 files in total

# Directory path for Log files
log_path = r"C:\sfb_load\log\\"
log_file = log_path + "sfb_file_transform."
log_file_name = str(log_file + datetime.datetime.now().strftime('%d-%m-%y') + ".log")


def check_directory():
    '''
    Checks and validates if the key working directories are present.
    directories being validated = log and input directory.
    :return:None
    '''
    if os.path.isdir(log_path):
        pass
    else:
        os.mkdir(log_path)
        log.log_me(log_file_name,"Directory " + log_path + " created.")

    if os.path.isdir(input_dir):
        pass
    else:
        os.mkdir(input_dir)
        log.log_me(log_file_name,"Directory " + input_dir + " created. However no csv files to process")
        log.log_me(log_file_name, "Terminating program..")
        exit(1)

    if os.path.isdir(output_dir):
        pass
    else:
        os.mkdir(output_dir)
        log.log_me(log_file_name,"Directory " + output_dir + " created.")


def split_file(input_file, suffix):
    """
    The file processes the input CSV file and creates the FEED and COMMENTS split.
    :param input_file: Name of the CSV file to be split into Feed and Comments data.
    :param suffix: technical parameters appended to the end of the split file. avoids overwriting.
    :return: None.
    """
    file_feed = output_dir + "FeedItem_"
    file_comment = output_dir + "FeedComment_"

    try:
        ''' Read the input CSV file'''
        data = pd.read_csv(input_file, header=None)
        tot_count = str(data[0].count())

        #Cleanse data remove tags
        tags = open(tag_param_for_removal, 'r')
        for tag in tags:
            if tag[0] == '#':
                pass
            else:
                data = data.replace(tag.strip(), '', regex=True)
        tags.close()
        log.log_me(log_file_name, "Tags configured in file : " + tag_param_for_removal + " removed.")
        log.log_me(log_file_name, "cleansing complete..")


        # Splitting datafram
        # Condition "2nd field = null" (Question)
        dataframe_feed = data[data[1].isnull()]
        dataframe_comment = data[data[1].notnull()]

        file_feed = file_feed + str(suffix) + ".csv"
        file_comment = file_comment + str(suffix) + ".csv"

        # Write back transformed file to "csv" format.
        # All parameters except "quoting" changed to "QUOTE_ALL" to match with the input quoting style.
        dataframe_feed.iloc[:, :13].to_csv(path_or_buf=file_feed, sep=',',
                                      na_rep='', float_format=None, columns=None, header=False, index=False,
                                      index_label=None, mode='w', encoding=None, compression=None, quoting=QUOTE_ALL,
                                      quotechar='"', line_terminator='\n', chunksize=None, tupleize_cols=None,
                                      date_format=None, doublequote=True, escapechar=None, decimal='.')
        dataframe_comment.iloc[:, :13].to_csv(path_or_buf=file_comment, sep=',',
                                      na_rep='', float_format=None, columns=None, header=False, index=False,
                                      index_label=None, mode='w', encoding=None, compression=None, quoting=QUOTE_ALL,
                                      quotechar='"', line_terminator='\n', chunksize=None, tupleize_cols=None,
                                      date_format=None, doublequote=True, escapechar=None, decimal='.')

        # Gather total number of lines for each of the files after split
        # purpose: logging.

        feed_count = str(dataframe_feed[0].count())
        comment_count = str(dataframe_comment[0].count())

        # Logging
        log.log_me(log_file_name, "Input File :" + input_file + ' | Total record count :' + tot_count)
        log.log_me(log_file_name, "File " + file_feed + ' created | Total feed count :' + feed_count)
        log.log_me(log_file_name, "File " + file_comment + ' created | Total feed count :' + comment_count)
        log.log_me(log_file_name, "----------------------------------")

    except Exception as e:
        log.log_me(log_file_name, "Exception encountered")
        log.log_me(log_file_name, e)


def scan_files():
    """
    Scan thought ALL files in input_directory,
    pick the "csv" files for processing (splitting).
    :return: Total number of files successfully processed.
    """
    try:
        counter = 0
        log.log_me(log_file_name, "=================================")
        log.log_me(log_file_name, "Input Directory :" + input_dir)
        log.log_me(log_file_name, "Output Directory :" +output_dir)
        log.log_me(log_file_name, "=================================")
        for file in os.listdir(input_dir):

            # Generate random 4 digit suffix to append to the split file name
            # Purpose: Avoid overwriting.
            suffix = rd.randint(1111, 9999)
            fin_inp_path = input_dir + file

            if file[-3:] == "csv":
                log.log_me(log_file_name, "Processing file : " + fin_inp_path)
                split_file(fin_inp_path, suffix)
                counter += 1
            else:
                continue
        return counter
    except Exception as e:
        log.log_me(log_file_name, "Exception encountered")
        log.log_me(log_file_name, e)


if __name__ == '__main__':
    # Validate if the configured directories exist.
    check_directory()

    # Call function to scan all files (csv's) within input directory
    no_of_files_processed = scan_files()
    print("Number of files processed :" + str(no_of_files_processed))
    print("Processing complete..")
    print("See log for details : " + log_file_name)