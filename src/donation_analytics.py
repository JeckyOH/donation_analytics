#! /usr/bin/env python

"""This is the running module.
Briefly explain my program:
In logic, I devided the entire program into 3 parts:
1) Frame part: here just the main function, which invokes other handlers, including read_file();
2) Input Module: here just the read_file() function, which deals with the input data and transform the input data into internal data structure.
   In this module, I use callback functions to invoke handlers. In this way, I can get the loose connection between Input Module and Handler Module.
3) Hander Module which performs main functionality of this program.
   In this module, I implemented two kinds of methods: the first one(handler_single_thread) uses single thread while the second one(handler_multi_threads) uses multiple threads to let different threads be responsible for IO intensive and CPU intensive tasks.

[PS]
When calculating given percentile, it is neccesary to get the sorted data structure of transaction amount. So, why not use self-sorted data structure to store those transaction amount identified by 'CMTE_ID|ZIPCODE|YEAR'. Thus, I use AVL Tree as data struction of transaction amount.
"""

import argparse
from datetime import datetime
import os
import sys
import time

# Handler Module, Single Thread
import handler_single_thread as HandlerSingleThread
# Handler Module, Multiple Threads
import handler_multi_threads as HandlerMultiThreads

# The position of specified columns in the input file.
CMTE_ID_POSITION = 0
NAME_POSITION = 7
ZIPCODE_POSITION = 10
TRANSACTION_DT_POSITION = 13
TRANSACTION_AMT_POSITION = 14
OTHER_ID_POSITION = 15

def get_args():
    """ Parse user auguments from command line. """

    usage_desc = """ Used for outputing information of political contributions and donors.  """
    parser = argparse.ArgumentParser(description=usage_desc)

    parser.add_argument('-d', '--datainput', type = str, action = 'store', dest = 'data_input_path', default = '../input/itcont.txt', help = "Path to data input files.")
    parser.add_argument('-p', '--percentileinput', type = str, action = 'store', dest = 'percentile_input_path', default = '../input/percentile.txt', help = "Path to percentile input files.")
    parser.add_argument('-o', '--output', type = str, action = 'store', dest = 'output_path', default = '../output/repeat_donors.txt', help = "Path to put output files.")
    parser.add_argument('-m', '--mode', type = str, action = 'store', dest = 'mode', default = 'single', help = "Mode: multi | single | both")
    parser.add_argument('-V', action = 'store_true', dest = 'verbose', default = False, help = "Enable the display of running time.")

    args = parser.parse_args()
    return args

def is_valid_date(date_str):
    """ Check whether a string is in valid date format.  """
    try:
        datetime.strptime(date_str, '%m%d%Y')
        return True
    except:
        return False

def is_number(s):
    """ Check whether a string is a number. """
    try:
        float(s) # for int, long and float
    except ValueError:
        return False
    return True

def read_file(usrargs, handler):
    """ Input Module.
    This module is responsible for dealing with dirty input data, including santinizing input data and transform these data into internal data format.
    """
    input_data_file = open(usrargs.data_input_path, 'r')
    input_percentile_file = open(usrargs.percentile_input_path, 'r')

    # Read the given percentile to be calculated
    line = input_percentile_file.readline()
    input_percentile_file.close()
    if not is_number(line):
        raise ValueError('Please give a number to indicate percentile.')
    percentage = float(line)

    # For each line of input data, santinize and transform it into internal format.
    for line in input_data_file:
        line_items = line.split('|')

        # If the OTHER_ID column is NOT empty, ignore entire record.
        if line_items[OTHER_ID_POSITION]:
            continue
        # If there is empty fields, ignore entire record.
        if not line_items[CMTE_ID_POSITION] or not line_items[NAME_POSITION]:
            continue
        # If TRANSACTION_AMT column is invalid, ignore entire record.
        if not line_items[TRANSACTION_AMT_POSITION] or not is_number(line_items[TRANSACTION_AMT_POSITION]):
            continue
        # If TRANSACTION_DT column is invalid, ignore entire record.
        if not line_items[TRANSACTION_DT_POSITION] or not is_valid_date(line_items[TRANSACTION_DT_POSITION]):
            continue
        # If ZIPCODE column is invalid, ignore entire record.
        if not line_items[ZIPCODE_POSITION] or len(line_items[ZIPCODE_POSITION]) < 5:
            continue;


        # I use dictionary as internal data format. For scalability, it can be modified to JSON format.
        distilled_data = {"cmte_id" : line_items[CMTE_ID_POSITION],
                          "zipcode" : line_items[ZIPCODE_POSITION][0:5],
                          "name" : line_items[NAME_POSITION],
                          "transaction_year" : datetime.strptime(line_items[TRANSACTION_DT_POSITION], '%m%d%Y').year,
                          "transaction_amt" : float(line_items[TRANSACTION_AMT_POSITION])}

        # Invoke handlers to perform functionality on the data. This is a callback function.
        handler(distilled_data, percentage, usrargs)

if __name__ == '__main__':
    try:
        usr_args = get_args()
        # Make sure path of input files is valid.
        if os.path.isfile(usr_args.data_input_path) and os.path.isfile(usr_args.percentile_input_path):

            # Use single thread handler.
            if usr_args.mode != 'multi':
                start_time = time.time() * 1000

                read_file(usr_args, HandlerSingleThread.handler)
                HandlerSingleThread.clean()

                running_time = time.time() * 1000 - start_time
                if usr_args.verbose:
                    print "For single thread handler, the running time is %f ms." %(running_time)

            # Use multiple threads handler.
            if usr_args.mode != 'single':
                start_time = time.time() * 1000

                read_file(usr_args, HandlerMultiThreads.handler)
                HandlerMultiThreads.clean()

                running_time = time.time() * 1000 - start_time
                if usr_args.verbose:
                    print "For multiple threads handler, the running time is %f ms." %(running_time)

        else:
            print ("The input file or ouput directory does NOT exist.\n")
    except:
        import sys
        sys.exit(1)
