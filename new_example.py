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

#
#
#'''
## TODO :  what should be queue type ???
## multiprocessing.managers.AutoProxy[Queue]
#
#def status_queueListener(queue, process_status_file: str) -> None:
#    #  manager.Queue()
#    # print("status logger")
#    # print(type(queue), ",", type(process_status_file))
#
#    """
#    fieldnames = ['name', 'branch', 'year', 'cgpa']
#    #writer = csv.writer(file)
#    # for writing headers
#    writer = csv.writer(file, fieldnames=fieldnames)
#    """
#
#    process_status_filehandle_writer = open(process_status_file, "w")
#    # TODO : add headers
#
#    # run forever
#    while True:
#        # print("checking queue for new message ")
#        sleep(1)
#
#        # consume a log message, block until one arrives
#        message = queue.get()
#
#        # check for shutdown
#        if message is None:
#            print("\nstatus logger > received None message , terminating listener ")
#            process_status_filehandle_writer.close()
#            break
#
#        process_status_filehandle_writer.write(message + "\n")
#
#    process_status_filehandle_writer.close()
#    return
#
#
## executed in a process that performs logging
#def logger_queueListener(queue, process_logger_file: str) -> None:
#    print("process  logger")
#    print(type(queue), ",", type(process_logger_file))
#
#    # create a logger
#    logger = logging.getLogger("processRootLogger")
#    logger.setLevel(logging.DEBUG)
#
#    # # create and add file handler
#    file_handler = logging.FileHandler(process_logger_file)
#    file_handler.setLevel(logging.INFO)
#    formatter = logging.Formatter(
#        "%(asctime)s %(levelname)s %(filename)s  %(funcName)s : %(lineno)d - %(message)s"
#    )
#    file_handler.setFormatter(formatter)
#    logger.addHandler(file_handler)
#
#    # create and add strem handler
#    stream_handler = logging.StreamHandler()
#    stream_handler.setLevel(logging.ERROR)
#    stream_handler.setFormatter(formatter)
#    logger.addHandler(stream_handler)
#
#    """
#    # Log message of severity INFO or above will be handled
#    logger.debug('Debug message')
#    logger.info('Info message')
#    logger.warning('Warning message')
#    logger.error('Error message')
#    logger.critical('Critical message')
#    """
#
#    # run forever
#    while True:
#        # consume a log message, block until one arrives
#        log_level, message = queue.get()
#
#        # print (f"inside process logging \t log level {log_level}")
#        # print (f"inside process logging \t log level {log_level}:{message}")
#        # sleep(1)
#
#        # check for shutdown
#        if message is None:
#            break
#
#        """
#        CRITICAL 50
#        ERROR 40
#        WARNING 30 
#        INFO 20
#        DEBUG 10
#        """
#
#        # log the message
#        # levelInteger =  logging.getLevelNamesMapping(log_level)
#        # print("\n levelInteger : ", levelInteger )
#        logger.log(log_level, message)
#
#    return
#
#
#def progress_bar_queueListener(queue, total_count) -> None:
#    t = tqdm(total=total_count, colour="RED")
#
#    # run forever
#    while True:
#        # print("checking queue for new message ")
#        sleep(1)
#
#        # consume a log message, block until one arrives
#        message = queue.get()
#
#        t.update(1)
#
#        # check for shutdown
#        if message is None:
#            print("\nstatus logger > received None message , terminating listener ")
#            break
#
#    t.close()
#'''
#


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

    # TODO : not sure what is the purpose
    set_start_method("spawn")

    # create the manager
    manager = Manager()

    # create shared queues
    logger_queue = manager.Queue()
    status_queue = manager.Queue()
    progress_bar_queue = manager.Queue()

    # starting process logger
    print("\nStarting logger queue listener...")
    Process(
        target=logger_queueListener,
        args=(
            logger_queue,
            f"{arguments.output_directory}/{arguments.process_logger_filename}",
        ),
    ).start()
    print("Listener process annotation : ", logger_queueListener.__annotations__)

    # starting status logger
    print("\nStarting status queue listener...")
    Process(
        target=status_queueListener,
        args=(
            status_queue,
            f"{arguments.output_directory}/{arguments.process_status_filename}",
        ),
    ).start()
    print("Listener process annotation : ", status_queueListener.__annotations__)

    count = 100

    # starting progress bar logger
    print("\nStarting progress bar queue listener...")
    Process(
        target=progress_bar_queueListener,
        args=(progress_bar_queue, count),
    ).start()
    print("Listener process annotation : ", progress_bar_queueListener.__annotations__)

    # create task id and a random payload
    task_id = [x for x in range(10)]
    task_payload = [random.randint(1, 10) for _ in range(10)]

    poolCount = 1

    # start pool process
    with Pool(processes=poolCount) as pool:
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

    print("\n Sending None message to queue ")
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
