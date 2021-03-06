import multiprocessing as mp
from datetime import datetime as dt
import time
import requests
import logging #TODO: not implemented yet!
import random
import os
from pprint import pprint


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
def task_producer(queue, lock, tasks):
    #lock here ensures only one process is writing to the console at a time.
    with lock: #TODO: use decorator for these prints.
        now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        print('[{}] Task producer {} STARTED.'.format(now, os.getpid()))

    # Place urls on the Queue
    for task in tasks:
        time.sleep(random.randint(1, 10)) #This should be deleted for PROD.
        queue.put(task)

    with lock:
        now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        print('[{}] Task producer {} DONE.'.format(now, os.getpid()))


"""
This consumer function pops items off the Queue.
"""
def task_consumer(queue, lock):
    with lock: #TODO: use decorator for these prints.
        now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        print('[{}] Task Consumer {} STARTED.'.format(now, os.getpid()))

    # Run indefinitely
    while True:
        time.sleep(random.randint(1, 10))

        #If queue is empty - get() will wait until queue has tasks.
        popped = queue.get() # pops task tuple (id, url)
        print(popped)
        url, url_id = popped, "1234"

        # fetch urls and return urlid, url, status code, url content
        rurlid, rurl, rcode, rcont[0:15], e = url_get(url,url_id)
        pprint ({'URLID': rurlid,
                 'URL': url,
                 'STATUS': rcode,
                 'CONTENT_SAMPLE': rcont,
                 'ERROR': e
                 })

        with lock: #TODO: use decorator for these prints.
            print('Consumer {} got {}'.format(os.getpid(), url_id))


if __name__ == '__main__':
    #mp.log_to_stderr(logging.DEBUG) #can change logging level

    # Generates placeholder taskid for the urls
    r = random.randint(1000,9999) #placeholder
    # List of urls, should be a queue dynamically appended by critters on PROD.
    tasks = ["https://www.patek.com/en/retail-service/patek-philippe-salons#geneva",
             "https://www.hugoboss.com/ch/fr/boss-homme-collection-defile/",
             "http://www.microsoft.com/",
             "https://www.python.org/",
             "https://github.com/"]

    # Create the Queue
    queue = mp.Queue()
    lock = mp.Lock()

    producers = []
    consumers = []

    for task in tasks:
        # Creates our producer processes by passing the producer function and it's arguments
        # On PROD the task list will be dynamically appended and a preset number
        # of producers will keep taking from a URLSTOPARSE queue instead of a list
        producers.append(mp.Process(target=task_producer, args=(queue, lock, task)))

    # Creates consumer processes
    for i in range(len(tasks) * 2):
        p = mp.Process(target=task_consumer, args=(queue, lock))

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
