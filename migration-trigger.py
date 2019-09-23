#!/usr/bin/python

import swiftclient
import pprint
import Queue
import threading
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
OBJECT_WORKERS = 20


class ContainerProcessor(object):
    def __init__(self, admin_user, admin_pass, auth_url, storage_url):
        self.user = admin_user
        self.u_pass = admin_pass
        self.auth_url = auth_url
        self.storage_url = storage_url
        self.queue = Queue.Queue(OBJECT_WORKERS * 2)
        self.threads = []

        for i in range(OBJECT_WORKERS):
            t = threading.Thread(target=self.worker)
            t.start()
            self.threads.append(t)

    def worker(self):
        conn = swiftclient.client.Connection(
            authurl=self.auth_url,
            user=self.user,
            key=self.u_pass,
            tenant_name=self.user,
            auth_version='2.0',
            os_options={'object_storage_url': self.storage_url})

        while True:
            work = self.queue.get()
            if work is None:
                break
            try:
                print 'GET: %s/%s' % (work['container'], work['name'])
                conn.get_object(work['container'], work['name'])
            except Exception as e:
                print "error on %s/%s: %s" % (work['container'], work['name'],
                                              repr(e))
                sys.exc_clear()
            self.queue.task_done()

    def stop(self):
        print "Stop called"
        self.queue.join()
        for i in range(OBJECT_WORKERS):
            self.queue.put(None)
        for t in self.threads:
            t.join()


def get_Config():
    global admin_user
    global admin_pass
    global source_auth_url
    global n_tenant_threads
    global account_list
    global source_swift_base

    config = ConfigParser.ConfigParser()
    config.read(r'migration-trigger.conf')
    admin_user = config.get('global', 'admin_user')
    admin_pass = config.get('global', 'admin_pass')
    source_auth_url = config.get('global', 'source_auth_url')
    n_tenant_threads = config.getint('global', 'n_tenant_threads')
    account_list = config.get('global', 'account_list')
    source_swift_base = config.get('global', 'source_swift_base')


class tenantThread(threading.Thread):
    def __init__(self, threadID, name, q,):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        process_tenant_thread(self.name, self.q)


def process_tenant_thread(threadName, q):
    while not exitFlag:
        data = q.get()
        if data is None:
            break
        process_tenant(data)


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

    print "admin_user:", admin_user
    print "admin_pass:", admin_pass
    print "source_auth_url:", source_auth_url
    print "account_list:", account_list
    print "source_swift_base:", source_swift_base

    processor = ContainerProcessor(admin_user, admin_pass, source_auth_url,
                                   source_swift_base + data[1])

    container_marker = ''
    conn = swiftclient.client.Connection(
        authurl=source_auth_url,
        user=admin_user,
        key=admin_pass,
        tenant_name=admin_user,
        auth_version='2.0',
        os_options={'object_storage_url': source_swift_base+data[1]})
    _, containers = conn.get_account(marker=container_marker)
    while containers and containers[-1]['name'] != container_marker:

        for sc in containers:
            marker = ''
            _, oblist = conn.get_container(sc['name'], marker=marker)
            print 'looking in', sc['name']
            get_count = 0
            skip_count = 0
            while oblist and oblist[-1]['name'] != marker:
                for ob in oblist:
                    cl = ob.get('content_location')
                    if cl and 'swift' not in cl:
                        processor.queue.put({'container': sc['name'],
                                             'name': ob['name']})
                        get_count += 1
                    else:
                        skip_count += 1
                marker = oblist[-1]['name']
                _, objlist = conn.get_container(
                    sc['name'], marker=marker)
            print "enqueued %d objects in %s, skipped %d objects" % (
                get_count, sc['name'], skip_count)
        container_marker = containers[-1]['name']
        _, containers = conn.get_account(marker=container_marker)

    processor.stop()


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
    workQueue = Queue.Queue(0)  # queue size is infinite
    threads = []
    threadID = 1

    # Create new threads
    for tName in tenantThreadList:
        thread = tenantThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    # Fill the queue

    # for word in nameList:
    for word in namedict.iteritems():
        workQueue.put(word)

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
