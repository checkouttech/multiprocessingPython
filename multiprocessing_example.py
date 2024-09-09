import argparse
from pathlib import Path
import datetime
from time import sleep
from multiprocessing import Pool, Manager, Process
from multiprocessing import set_start_method, current_process
import logging
import random
import itertools
from queue_fuctions import  logger_queueListener,  status_queueListener,  progress_bar_queueListener 
from queue_fuctions import initalize_logger_queueListener, initalize_status_queueListener, initalize_progress_bar_queueListener

# CAUTION: not advised
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def parseArguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--approach", required=True, choices=["byFeatureID"])
    parser.add_argument(
        "-f",
        "--list-of-features-file",
        required=True,
        type=argparse.FileType("r", encoding="UTF-8"),
    )
    parser.add_argument("-a", "--authToken", required=True)
    parser.add_argument("-d", "--output-directory", type=Path)
    parser.add_argument("-c", "--count-parallel-worker-tasks", default=20)
    parser.add_argument("-s", "--sleep-tasks-in-sec", default=5)
    #  parser.add_argument('-o', '--output-filename', default ="result.csv")

    arguments = parser.parse_args()

    return arguments


def setup(arguments: argparse.Namespace) -> argparse.Namespace:
    print("\n in setup \n")

    output_dir: str

    # create time stamped based output directory if not provided
    if arguments.output_directory is not None:
        output_dir = arguments.output_directory
    else:
        now = datetime.datetime.now()
        output_dir = "./output_dir/output_dir_" + now.strftime("%Y%m%d_%H%M")

    # create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # copy output_dir to  arguments.output_directory
    setattr(arguments, "output_directory", output_dir)

    setattr(arguments, "process_logger_filename", "process.log")
    setattr(arguments, "process_status_filename", "process_status.csv")

    print(arguments)

    # return output_dir
    return arguments



def task(i: int, payload: str, logger_queue, status_queue, progress_bar_queue):
    # get the current process
    process = current_process()

    # generate some work
    s = random.randint(1, 10)

    # block to simulate work
    sleep(s)

    data = f"TASK function - {process} - {i} - sleep for {payload} sec "

    # print(data)

    # put it on the queue
    logger_queue.put((logging.INFO, data))

    status_queue.put(f"{i},DONE")

    progress_bar_queue.put(1)


def main() -> None:
    arguments = parseArguments()

    arguments = setup(arguments)

    # get input parameters 
    list_of_features_filename =  arguments.list_of_features_file
    feature_line_list = list_of_features_filename.readlines() 
    input_file_line_count = len ( feature_line_list)  


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
                task_id,
                task_payload,
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
    sleep(10)

    print("\n Sending None message to all queues ")
    # queue.put(None)

    # pass none to terminate status logger
    progress_bar_queue.put(None)

    # pass none to terminate status logger
    status_queue.put(None)

    # pass none to terminate status logger
    logger_queue.put((logging.INFO, "Ending process"))
    logger_queue.put(("FOOOOOOBARRRRRR", None))


if __name__ == "__main__":
    main()
