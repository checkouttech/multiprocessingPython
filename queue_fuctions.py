from tqdm import tqdm
import logging

#from multiprocessing import Manager
#queue = Manager.Queue() 

#from multiprocessing import Manager.Queue() 


#from multiprocessing import Manager
#manager = Manager()
#queue = manager.Queue()

# executed in a process that performs logging
# def status_logger(queue,arguments):
# multiprocessing.managers.AutoProxy[Queue]
# TODO :  what should be queue type ???

def progress_bar_queueListener(queue, total_count) -> None:
    t = tqdm(total=total_count, colour="RED")

    # run forever
    while True:
        # print("checking queue for new message ")
        #sleep(1)

        # consume a log message, block until one arrives
        message = queue.get()

        t.update(1)

        # check for shutdown
        if message is None:
            print("\nstatus logger > received None message , terminating listener ")
            break

    t.close()


def status_queueListener(queue, process_status_file: str) -> None:
    #  manager.Queue()
    # print("status logger")
    # print(type(queue), ",", type(process_status_file))

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
        # print("checking queue for new message ")
        #sleep(1)

        # consume a log message, block until one arrives
        message = queue.get()

        # check for shutdown
        if message is None:
            print("\nstatus logger > received None message , terminating listener ")
            process_status_filehandle_writer.close()
            break

        process_status_filehandle_writer.write(message + "\n")

    process_status_filehandle_writer.close()
    return


# executed in a process that performs logging
def logger_queueListener(queue, process_logger_file: str) -> None:
    print("process  logger")
    print(type(queue), ",", type(process_logger_file))

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

        # print (f"inside process logging \t log level {log_level}")
        # print (f"inside process logging \t log level {log_level}:{message}")
        # sleep(1)

        # check for shutdown
        if message is None:
            break

        """
        CRITICAL 50
        ERROR 40
        WARNING 30 
        INFO 20
        DEBUG 10
        """

        # log the message
        # levelInteger =  logging.getLevelNamesMapping(log_level)
        # print("\n levelInteger : ", levelInteger )
        logger.log(log_level, message)

    return


