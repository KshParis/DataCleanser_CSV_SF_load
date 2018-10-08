#!/usr/bin/env python3

# Author: krishna.r.shetty@outlook.com
# Version: 2.0
# 08-Oct-2018.

import pandas as pd
import random as rd
import os
import write_log as log
import datetime
from csv import QUOTE_ALL  # package to add quote for all fields in the final files.
import numpy as np # for column 7, replacing if length > 9950 and add custom text


# CONFIGURATION
# File containing all the pre identified HTML tags that needs to be removed before SF loading.
tag_param_for_removal = r"C:\Users\krishna.shetty\PycharmProjects\SFBLoad\config\parameters.txt"

# Directory that contains all source CSV files that needs to be cleansed.
input_dir = r"C:\sfb_load\input\\"
sf_suc_file_feed_item = r"C:\sfb_load\input\sf\sfFeedItemSuccess.csv" #directory containing post processed SF file.
sf_item_file = r"C:\sfb_load\input\sf\FeedItem.csv"
sf_comment_file = r"C:\sfb_load\input\sf\FeedComment.csv"

#File header (post split)
item_header = ['messageid','parentmessageid','threadid','containertype','containerid','userid','subject','body',
               'rewardpoints','creationdate','modificationdate','status','isRichText','Type']

comment_header = ['messageid','parentmessageid','threadid','containertype','containerid','userid','subject',
                  'body','rewardpoints','creationdate','modificationdate','status','isRichText']


# Directory where the cleansed, transformed files will be generated.
output_dir = r"C:\sfb_load\output\\"  # Folder location where the split file will be stored. 2 files in total

# Directory path for Log files
log_path = r"C:\sfb_load\log\\"
log_file = log_path + "sfb_file_transform."
log_file_name = str(log_file + datetime.datetime.now().strftime('%d-%m-%y') + ".log")

#Custom file changes
custom_text = 'The original post was too long, and is attached as a text file'


def to_csv(DataFrame, FileName, Mode = 'a', header = None):
    '''
    :param DataFrame: Name of the active pandas datafrmae.
    :param FileName: target file name to which the dataframe will be written to.
    :param Mode: default mode - Append (a), unless specificed as "write" (w).
    :return: None.
    '''
    DataFrame.to_csv(path_or_buf=FileName, sep=',',
                     na_rep='', float_format=None, columns=None, header=header, index=False,
                     index_label=None, mode=Mode, encoding=None, compression=None, quoting=QUOTE_ALL,
                     quotechar='"', line_terminator='\n', chunksize=None, tupleize_cols=None,
                     date_format=None, doublequote=True, escapechar=None, decimal='.')
    print('file created : ' + FileName)

def add_header(file_name, ftype):

    if ftype == 'c':
        df_tmp = pd.read_csv(file_name, header=None)
        os.remove(file_name)
        to_csv(DataFrame=df_tmp, FileName=file_name, Mode='w', header=comment_header)
    elif ftype == 'i':
        df_tmp = pd.read_csv(file_name, header=None)
        to_csv(DataFrame=df_tmp, FileName=file_name, Mode='w', header=item_header)
    else:
        pass


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

    print("Directory check : OK")

def modify_feed(feed_file):
    os.chdir(output_dir)
    df = pd.read_csv(feed_file)
    df[13] = r'QuestionPost'

    to_csv(df, feed_file, 'w')
    print("file modified "+ feed_file)


def split_file(input_file, suffix):
    """
    The file processes the input CSV file and creates the FEED and COMMENTS split.
    :param input_file: Name of the CSV file to be split into Feed and Comments data.
    :param suffix: technical parameters appended to the end of the split file. avoids overwriting.
    :return: None.
    """
    print("Processing file : " + input_file)
    file_feed = output_dir + "FeedItem_"
    file_comment = output_dir + "FeedComment_"

    try:
        ''' Read the input CSV file'''
        data = pd.read_csv(input_file, header=None, index_col=None)
        tot_count = str(data[0].count())

        #Text manipulation if length > 9950
        data[7] = np.where(data[7].str.len() > 9950, data[7].str.slice(0, 1000), data[7])

        #Date conversion
        data[9] = pd.to_datetime(data[9], unit='ms')
        data[10] = pd.to_datetime(data[10], unit='ms')
        data[12] = r'TRUE'

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


        # Splitting dataframe
        # Condition "2nd field = null" (Question)
        dataframe_feed = data[data[1].isnull()]
        dataframe_comment = data[data[1].notnull()]

        file_feed = file_feed + str(suffix) + ".csv"
        file_comment = file_comment + str(suffix) + ".csv"

        # Write back transformed file to "csv" format.
        # All parameters except "quoting" changed to "QUOTE_ALL" to match with the input quoting style.
        to_csv(dataframe_feed, file_feed, 'w')
        to_csv(dataframe_comment, file_comment, 'w')

        # Gather total number of lines for each of the files after split
        # purpose: logging.
        feed_count = str(dataframe_feed[0].count())
        comment_count = str(dataframe_comment[0].count())

        #Post split modifications
        modify_feed(file_feed)

        #add header
        add_header(file_feed, 'i')
        add_header(file_comment, 'c')

        # Logging
        log.log_me(log_file_name, "Input File :" + input_file + ' | Total record count :' + tot_count)
        log.log_me(log_file_name, "File " + file_feed + ' created | Total feed count :' + feed_count)
        log.log_me(log_file_name, "File " + file_comment + ' created | Total feed count :' + comment_count)
        log.log_me(log_file_name, "----------------------------------")
        print("File "+ input_file +" processed")

    except Exception as e:
        log.log_me(log_file_name, "Exception encountered")
        log.log_me(log_file_name, e)
        exit(1)


def scan_files():
    """
    Scan thought ALL files in input_directory,
    pick the "csv" files for processing (splitting).
    :return: Total number of files successfully processed.
    """
    try:
        print("Initializing file processing")
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
        exit(1)


def MergeAll():
    '''
    execute as follows:
    sfb_load.MergeAll()
    :return:
    '''
    os.chdir(output_dir)
    feed_master_file = 'feed_master_'
    comment_master_file = 'comment_master_'

    #suffix = rd.randint(1111, 9999)
    #suffix = '123'

    #fm_final = feed_master_file + str(suffix) + '.csv'
    #cm_final = comment_master_file + str(suffix) + '.csv'

    fm_final = feed_master_file + '.csv'
    cm_final = comment_master_file +  '.csv'

    print(fm_final)
    print(cm_final)

    for file in os.listdir(output_dir):
        if file[:7] == "FeedIte":
            print('processing file : ' + file)
            tmp_df = pd.read_csv(file, header=None)
            to_csv(tmp_df, fm_final, 'a')

        elif file[:7] == "FeedCom":
            print('processing file : ' + file)
            tmp_df_cm = pd.read_csv(file, header=None)
            to_csv(tmp_df_cm, cm_final, 'a')
        else:
            pass


def post_load_transformation():
    ''' Perform post split file modification '''
    # read sf input csv file'''
    # header_modification
    comment_header = ['messageid', 'parentmessageid', 'threadid', 'containertype', 'containerid', 'userid', 'subject',
                      'body', 'rewardpoints', 'creationdate', 'modificationdate', 'status',
                      'isRichText','id', 'ParentThreadL0']


    df_sf = pd.read_csv(sf_suc_file_feed_item, encoding = "ISO-8859-1")
    df_comm = pd.read_csv(sf_comment_file, encoding = "ISO-8859-1")

    df_comm = pd.merge(df_comm, df_sf[['threadid', 'id', 'messageid']], on='threadid', how='left')
    to_csv(df_comm, sf_comment_file, 'w', comment_header)
    #df_comm.to_csv(sf_comment_file, sep=',', na_rep='', float_format=None, header=True, mode='w', index=False, index_label=None)


if __name__ == '__main__':
    # Validate if the configured directories exist.
    check_directory()

    # Call function to scan all files (csv's) within input directory
    #no_of_files_processed = scan_files()
    #print("Number of files processed :" + str(no_of_files_processed))

    #Merging splited files into one single file for feed and comments.
    #Merge_feedandcomment()

    #Perform post SF load transformation
    post_load_transformation()
    print('Post load transformation completed..')


    print("Processing complete..")
    print("See log for details : " + log_file_name)