import argparse
from pathlib import Path
import datetime
from time import sleep
from multiprocessing import Pool, Manager, Process
from multiprocessing import set_start_method, current_process
import logging
import random
import itertools
import uuid
import csv
import os
from queue_fuctions import  logger_queueListener,  status_queueListener,  progress_bar_queueListener 
from queue_fuctions import initalize_logger_queueListener, initalize_status_queueListener, initalize_progress_bar_queueListener
import copy
import json
from tqdm import tqdm
import sys
import requests 
import boto3 

# CAUTION: not advised
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def parseArguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    # parser.add_argument("--approach", required=True, choices=["byFeatureID"])
    parser.add_argument(
        "-f",
        "--input-filename",
        required=True,
        # type=argparse.FileType("r", encoding="UTF-8"),
    )
    parser.add_argument("-i", "--run-id", nargs="?", const=None)



    parser.add_argument("-a", "--authToken", required=True)






    # # Define an argument that accepts multiple values


    ids=[
        "first_id",
        "second_id",
        "third_id",
        "fourth_id",
        "fifth_id",
        "sixth_id",
        "some-random-text",
        "option-23",
        "option-1",
        "choice-3"
    ]

    # Define an argument that accepts one or multiple values from the list of ids 
    parser.add_argument("-l", "--layer_ids", nargs="+", help="List of id", choices=ids, default=ids ) 




    parser.add_argument("--service_endpoint", type=validate_url_format, default="http://foobar.com") 
    parser.add_argument("-d", "--output-directory", type=Path)
    parser.add_argument("-c", "--count-parallel-worker-tasks",  type=int, default=50)
    parser.add_argument("-s", "--sleep-tasks-in-sec",  type=int, default=5)

    arguments = parser.parse_args()

    return arguments


def setup(arguments: argparse.Namespace) -> argparse.Namespace:
    print("\n in setup \n")

    if os.path.exists(arguments.input_filename) and os.access(arguments.input_filename, os.R_OK):
        print("File exists and is readable")
    else:
        print("File does not exist or is not readable")
        exit()

    # Output Directory and status  / log files 
    # create time stamped based output directory if not provided
    if arguments.output_directory is None:
        now = datetime.datetime.now()
        setattr(arguments, "output_directory",  os.path.normpath(  "./output/" + now.strftime("%Y%m%d_%H%M") + "_dir")) 

    # create output directory for process logs 
    Path(arguments.output_directory).mkdir(parents=True, exist_ok=True)

    setattr(arguments, "process_logger_filename", "process.log")
    setattr(arguments, "process_status_filename", "process_status.csv")

    # set run id ( to be copied to payload if possible , for splunk tracking purposes  )
    if arguments.run_id is None or arguments.run_id == "UUID":
        run_id = str(uuid.uuid4())
        setattr(arguments, "run_id", run_id)

    # assign value needed  
    print(arguments)

    return arguments



def task( 

    input_dataset_one_row_dict,
    arguments,
    logger_queue,
    status_queue,
    progress_bar_queue,
                                              ):
    # create unique identifier for each task,  For tracking purposes 
    unique_task_identifier = f"{input_dataset_one_row_dict['id']}"

    sleep(arguments.sleep_tasks_in_sec)

    print (input_dataset_one_row_dict ) 


    logger_queue.put((logging.INFO, f"ID - {unique_task_identifier} : {input_dataset_one_row_dict}"))


    # get the current process
    process = current_process()

    # generate some work
    s = random.randint(1, 10)

    # block to simulate work
    sleep(s)

    data = f"TASK function - {process} - {i} - sleep for {payload} sec "

    # print(data)

    # put it on the queue
    logger_queue.put((logging.INFO, f"ID - {unique_task_identifier} : data "))

    status_queue.put(f"{unique_task_identifier},DONE")

    progress_bar_queue.put(1)



def main() -> None:
    arguments = parseArguments()

    arguments = setup(arguments)

    print(" arguments : ", arguments)

    # get input parameters
    inputCsvFileHandle = open(arguments.input_filename, "r")
    header = [h.strip() for h in inputCsvFileHandle.readline().split(',')]
    reader = csv.DictReader(inputCsvFileHandle, delimiter=",", quotechar='"', fieldnames=header)

    feature_line_list = []
    rows_total = 0
    rows_valid = 0
    rows_invalid = 0

    # get all valid rows
    for row in reader:
        print(row)
        rows_total += 1
        cleaned_row = {k: v.strip() for k, v in row.items()}

        try:
            float(cleaned_row["latitude"])  # Assuming 'latitude' column should be a float
            float(cleaned_row["longitude"])  # Assuming 'longitude' column should be a float
            feature_line_list.append(cleaned_row)
            # feature_line_list.append ( [value for value in row.values()] )
            rows_valid += 1
        except ValueError:
            print("Invalid row:", row)
            rows_invalid += 1

    print("input csv file : ", arguments.input_filename)
    print("rows total : ", rows_total)
    print("rows valid : ", rows_valid)
    print("rows ignored  : ", rows_invalid)


    # TODO : not sure what is the purpose
    set_start_method("spawn")

    # create the manager
    manager = Manager()

    # create shared queues
    logger_queue       = initalize_logger_queueListener(manager, f"{arguments.output_directory}/{arguments.process_logger_filename}" )
    status_queue       = initalize_status_queueListener(manager, f"{arguments.output_directory}/{arguments.process_status_filename}" )
    progress_bar_queue = initalize_progress_bar_queueListener(manager, input_file_line_count) 

    # create task id and a random payload
    task_id = [x for x in range(10)]
    task_payload = [random.randint(1, 10) for _ in range(10)]

    # start pool process
    with Pool(processes=arguments.count_parallel_worker_tasks) as pool:
        pool.starmap(
            task,
            zip(
                feature_line_list,
                itertools.repeat(arguments),
                itertools.repeat(logger_queue),
                itertools.repeat(status_queue),
                itertools.repeat(progress_bar_queue),
            ),
        )

        # calling close as no more job to submit to pool
        pool.close()

        # calling join to wait for all pool process to terminate
        pool.join()

    # wait for all tasks to get over
    sleep(20)

    print("\n Sending None message to all queues ")
    # queue.put(None)

    # pass none to terminate progress bar logger
    progress_bar_queue.put(None)

    # pass none to terminate status logger
    status_queue.put(None)

    # pass none to terminate process logger
    logger_queue.put((logging.INFO, "Ending process"))
    logger_queue.put(("FOOOOOOBARRRRRR", None))

    print("\nOutput Directory: ", arguments.output_directory)


if __name__ == "__main__":
    main()
