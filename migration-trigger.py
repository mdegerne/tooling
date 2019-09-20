#!/usr/bin/python

import swiftclient
import pprint
import Queue
import threading
import time
import timeit
import ConfigParser
import sys

# global variant from config file
admin_user = ''
admin_pass = ''
source_auth_url = ''
source_swift_base = ''
n_tenant_threads = 1

# local variant use for code only
pp = pprint.PrettyPrinter(indent=4)
exitFlag = 0


def get_Config():
    global admin_user
    global admin_pass
    global source_auth_url
    global n_tenant_threads
    global account_list
    global source_swift_base
    global download_location

    config = ConfigParser.ConfigParser()
    config.read(r'migration-trigger.conf')
    admin_user = config.get('global', 'admin_user')
    admin_pass = config.get('global', 'admin_pass')
    source_auth_url = config.get('global', 'source_auth_url')
    n_tenant_threads = config.getint('global', 'n_tenant_threads')
    account_list = config.get('global', 'account_list')
    source_swift_base = config.get('global', 'source_swift_base')
    download_location = config.get('global', 'download_location')


class tenantThread(threading.Thread):
    def __init__(self, threadID, name, q, l,):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.L = l

    def run(self):
        process_tenant_thread(self.name, self.q, self.L)


def process_tenant_thread(threadName, q, l):
    while not exitFlag:
        l.acquire()
        if not q.empty():
            data = q.get()
            l.release()
            process_tenant(data)
        else:
            l.release()
        time.sleep(1)


def make_tenant_threadlist(int_tenant_threads):
    threadList = []
    for i in range(1, int_tenant_threads + 1):
        threadList.append('tenant Thread-' + str(i))

    return threadList


def process_tenant(data):
    global admin_user
    global admin_pass
    global source_auth_url
    global account_list
    global source_swift_base
    global download_location

    print admin_user
    print admin_pass
    print source_auth_url
    print account_list
    print source_swift_base
    print download_location

    try:
        print data
        # get source cluster connection by tenant
        swift_s = swiftclient.client.Connection(
            authurl=source_auth_url,
            user=admin_user,
            key=admin_pass,
            tenant_name=admin_user,
            auth_version='2.0',
            os_options={'object_storage_url': source_swift_base+data[1]})
        resp_headers, containers = swift_s.get_account()
        print data[1] + ": " + \
            str(float(resp_headers['x-account-bytes-used']))
        for sc in containers:
            print "container: " + str(sc['name'])
            resp_headers_s, objlist = swift_s.get_container(sc['name'])
            listing = objlist
            while listing:
                marker = listing[-1].get('name')
                # print "marker: " + marker
                resp_headers_s, objlist_again = swift_s.get_container(
                    sc['name'], marker=marker)
                listing = objlist_again
                if listing:
                    objlist.extend(listing)

            for so in objlist:
                print "GET object: " + str(so['name'])
                marker = str(so['name'])
                download_objoect(swift_s, sc['name'], so['name'],
                                 download_location)
        swift_s.close()
    except Exception as err:
        print data[1] + ": " + str(err)


def download_objoect(swift_x, container_name, object_name, download_location):
    try:
        # print "download all: " +container_name + ": " + object_name
        resp_headers, body = swift_x.get_object(container_name, object_name)
        # bolreturn = True
        # with open(download_location + '/' + object_name, 'w') as local:
        #    local.write(body)
    except Exception as err:
        print container_name, object_name, "error: ", str(err)
        sys.exc_clear()


def get_tenant_list(account_list):
    with open(account_list) as f:
        lines = f.readlines()
    tenant_list = [x.strip() for x in lines]
    return tenant_list


def main():
    global n_tenant_threads
    global admin_user
    global admin_pass
    global source_auth_url
    global account_list

    start = timeit.default_timer()

    # Get Config
    get_Config()

    # Get Tenant list
    tenant_list = get_tenant_list(account_list)
    tenantThreadList = make_tenant_threadlist(n_tenant_threads)

    # Generate tenant dictionary by tennant list
    namedict = {}
    namecount = 1
    for tenant in tenant_list:
        namedict[namecount] = tenant
        namecount += 1

    # Queue # = thread list
    queueLock = threading.Lock()
    workQueue = Queue.Queue(0)  # queue size is infinite
    threads = []
    threadID = 1

    # Create new threads
    for tName in tenantThreadList:
        thread = tenantThread(threadID, tName, workQueue, queueLock)
        thread.start()
        threads.append(thread)
        threadID += 1

    # Fill the queue
    queueLock.acquire()

    # for word in nameList:
    for word in namedict.iteritems():
        workQueue.put(word)
    queueLock.release()

    # Wait for queue to empty
    while not workQueue.empty():
        pass

    # Notify threads it's time to exit
    global exitFlag
    exitFlag = 1

    # Wait for all threads to complete
    for t in threads:
        t.join()

    print 'Exiting Main Thread, Time Cost: %s' \
        % (timeit.default_timer() - start)


if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt):
        print "Abort, Got Keyboard Interrupt !"
        sys.exit(1)
