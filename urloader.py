from multiprocessing import Process, Queue, Lock
from datetime import datetime as dt
import requests
import logging #TODO: not implemented yet!
import random
import os


"""
This fetches the urls and returns the urlid, url, status code and url content.
"""
def url_get(url,url_id):
    try:
        res = requests.get(url)
        return url_id, url, res.status_code, res.text, None
    except Exception as e:
        return url_id, url, res.status_code, None, e


"""
This producer function puts items on the Queue.
"""
def task_producer(queue, lock, urls):
    #lock here ensures only one process is writing to the console at a time.
    with lock: #TODO: use decorator for these prints.
        now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        print('[{}] Task producer {} STARTED.'.format(os.getpid(), now))

    # Place urls on the Queue
    for task in tasks:
        time.sleep(random.randint(1, 3)) #This should be deleted for PROD.
        queue.put(task)

    with lock:
        now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        print('[{}] Task producer {} DONE.'.format(os.getpid(), now))


"""
This consumer function pops items off the Queue.
"""
def task_consumer(queue, lock):
    with lock: #TODO: use decorator for these prints.
        now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        print('[{}] Task Consumer {} STARTED.'.format(os.getpid(), now))

    # Run indefinitely
    while True:
        time.sleep(random.randint(1, 5))
        # If queue is empty - get() will wait until queue has tasks.
        task = queue.get()

        with lock: #TODO: use decorator for these prints.
            print('Consumer {} got {}'.format(os.getpid(), task[0])) #task[0]=id


if __name__ == '__main__':
    multiprocessing.log_to_stderr(logging.DEBUG) #can change logging level

    # OS backed random 6 digit int seed for creating url IDs, could have used hashes
    # List of urls, should be a queue dynamically appended by critters on PROD.
    tasks = [("https://www.patek.com/en/retail-service/patek-philippe-salons#geneva", os.urandom(6)),
            ("https://www.hugoboss.com/ch/fr/boss-homme-collection-defile/", os.urandom(6)),
            ("http://www.microsoft.com", os.urandom(6)),
            ("https://www.python.org/", os.urandom(6)),
            ("https://github.com", os.urandom(6))]

    # Create the Queue
    queue = Queue()
    lock = Lock()

    producers = []
    consumers = []

    for task in tasks:
        # Creates our producer processes by passing the producer function and it's arguments
        # On PROD the task list will be dynamically appended and a preset number
        # of producers will keep taking from a URLSTOPARSE queue instead of a list
        producers.append(Process(target=task_producer, args=(queue, lock, task)))

    # Creates consumer processes
    for i in range(len(tasks) * 2):
        p = Process(target=task_consumer, args=(queue, lock))

        # IMPORTANT! 'False' here will keep the process alive forever.
        p.daemon = True
        consumers.append(p)

    # One process spawned per one start() - multiprocessing yay!
    for p in producers:
        p.start()

    for c in consumers:
        c.start()

    # This allows the parent process to terminate after all of its children finish
    # Needed because consumers have a while(True)
    for p in producers:
        p.join()

    print('URL LOADER Parent Process FINISHED.')
