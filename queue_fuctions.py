from tqdm import tqdm
import logging
import sys 
#from tqdm.auto import tqdm

from multiprocessing import Process


# multiprocessing.managers.AutoProxy[Queue]
# TODO :  what should be queue type ???

def progress_bar_queueListener(queue, total_count) -> None:

    #tqdm_progress_bar = tqdm(total=total_count, colour="RED", position=0, leave=True , file=sys.stdout , desc="TASK(s) COMPLETION PROGRESS BAR")
    tqdm_progress_bar = tqdm(total=total_count, colour="RED", position=0,  file=sys.stdout , desc="TASK(s) COMPLETION PROGRESS BAR")

    # run forever
    while True:
        # "checking queue for new message "
        message = queue.get()
        #print ("inside progress_bar_queueListener " , message ) 

        # check for shutdown
        if message is None:
            print("\nReceived None message, terminating progress bar queue listener ")
            tqdm_progress_bar.close()
            return 

        if isinstance( message, int ):
            count = message
            tqdm_progress_bar.update(count)

    tqdm_progress_bar.close()
    return 


def status_queueListener(queue, process_status_file: str) -> None:
    """
    fieldnames = ['name', 'branch', 'year', 'cgpa']
    #writer = csv.writer(file)
    # for writing headers
    writer = csv.writer(file, fieldnames=fieldnames)
    """

    process_status_filehandle_writer = open(process_status_file, "w")
    # TODO : add headers

    # run forever
    while True:

        # consume a log message, block until one arrives
        message = queue.get()

        # check for shutdown
        if message is None:
            print("\nReceived None message, terminating status queue listener ")
            process_status_filehandle_writer.close()
            break

        process_status_filehandle_writer.write(message + "\n")

    process_status_filehandle_writer.close()
    return


def logger_queueListener(queue, process_logger_file: str) -> None:

    # create a logger
    logger = logging.getLogger("processRootLogger")
    logger.setLevel(logging.DEBUG)

    # # create and add file handler
    file_handler = logging.FileHandler(process_logger_file)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(filename)s  %(funcName)s : %(lineno)d - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # create and add strem handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    """
        CRITICAL 50
        ERROR 40
        WARNING 30 
        INFO 20
        DEBUG 10

    # Log message of severity INFO or above will be handled
    logger.debug('Debug message')
    logger.info('Info message')
    logger.warning('Warning message')
    logger.error('Error message')
    logger.critical('Critical message')
    """

    # run forever
    while True:
        # consume a log message, block until one arrives
        log_level, message = queue.get()

        # check for shutdown
        if message is None:
            print("\nReceived None message, terminating logger queue listener ")
            break
        logger.log(log_level, message)

    return



def initalize_logger_queueListener(manager, process_logger_filename) :

    logger_queue = manager.Queue()

    # starting process logger
    print("\nStarting logger queue listener...")
    Process(
        target=logger_queueListener,
        args=(
            logger_queue,
            process_logger_filename,
        ),
    ).start()
    print("Listener process annotation : ", logger_queueListener.__annotations__)

    return logger_queue

    



def initalize_status_queueListener(manager, process_status_filename) :

    status_queue = manager.Queue()

    # starting status logger
    print("\nStarting status queue listener...")
    Process(
        target=status_queueListener,
        args=(
            status_queue,
            process_status_filename,
        ),
    ).start()
    print("Listener process annotation : ", status_queueListener.__annotations__)

    return status_queue 


def initalize_progress_bar_queueListener(manager, task_count ):

    progress_bar_queue = manager.Queue()

    # starting progress bar logger
    print("\nStarting progress bar queue listener...")
    Process(
        target=progress_bar_queueListener,
        args=(progress_bar_queue, task_count),
    ).start()
    print("Listener process annotation : ", progress_bar_queueListener.__annotations__)
    
    return progress_bar_queue

