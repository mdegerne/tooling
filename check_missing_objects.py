import argparse
import datetime
import functools
import queue
import threading
import traceback
import urllib.parse
import random
import swiftclient
import sys
import time


RETRIES = 10


def iterate_listing(list_func):
    marker = ''
    while True:
        tries = 0
        while tries < RETRIES:
            try:
                _, listing = list_func(marker=marker)
                break
            except Exception as e:
                print('Iterate failed at marker={}: {}'.format(
                    marker, repr(e)))
                if tries == RETRIES:
                    raise e
            time.sleep(random.randint(1, 1000) * tries)
            tries += 1

        if not listing:
            break
        for entry in listing:
            yield(entry)
        marker = entry['name']
    yield None


def parse_last_modified(entry):
    return datetime.datetime.strptime(
        entry['last_modified'], '%Y-%m-%dT%H:%M:%S.%f')


def set_storage_url(conn, account):
    # parsed_url = urllib.parse.urlparse(conn.url)
    urllib.parse.urlparse(conn.url)
    print('Current connection URL: {}'.format(conn.url))
    path = '/v1/{}'.format(urllib.parse.quote(account))
    _, netloc, _, _, _ = urllib.parse.urlsplit(conn.url)
    storage_url = urllib.parse.urlunparse(('http', netloc, path, '', '', ''))
    print('Setting storage URL to: {}'.format(storage_url))
    conn.url = storage_url
    conn.os_options['object_storage_url'] = storage_url


def get_conn(authurl, user, key, account):
    conn = swiftclient.client.Connection(authurl=authurl, user=user, key=key)

    try:
        conn.get_auth()
        set_storage_url(conn, account)
    except Exception as e:
        print('Error authenticating to {}: {}'.format(authurl, e))
        return None
    return conn


def worker(config, work_queue):
    while True:
        results = []
        work = work_queue.get()
        if not work:
            work_queue.task_done()
            return

        account = work['account']
        client = get_conn(config.auth_url, config.user, config.key, account)

        if not client:
            work_queue.task_done()
            continue

        try:
            container_iter = iterate_listing(
                functools.partial(client.get_container, work['container']))

            try:
                entry = next(container_iter)
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    print('Missing container {}'.format(work['container']))
                    entry = None
                elif e.http_status == 401:
                    print('Not authorized to access {}'.format(
                        work['container']))
                    raise Exception('Destination unauthorized - skipping')
                else:
                    raise

            while entry:
                if 'content_location' in entry:
                    if 'swift' not in entry['content_location']:
                        results.append(entry)
                entry = next(container_iter)
            if results:
                print('Container {} has {} missing objects:'.format(
                    work['container'], len(results)))
                for entry in results:
                    print(entry['name'])
        except Exception:
            print(traceback.format_exc())
        finally:
            work_queue.task_done()


def containers_only(conn):
    for container in iterate_listing(conn.get_account):
        if not container:
            break
        if 'swift' not in container.get('content_location', []):
            print('Container {} with {} objects has not yet been '
                  'migrated'.format(container['name'], container['count']))


def main():
    parser = argparse.ArgumentParser(
        description='Aggregate stats for Swift accounts during migrations')
    parser.add_argument(
        '--container', type=str, required=False,
        help='Container to verify')
    parser.add_argument(
        '--account', type=str, required=True,
        help='Account to verify')
    parser.add_argument(
        '--workers', type=int, required=False, default=10,
        help='Number of concurrent requests')
    parser.add_argument(
        '--auth-url', type=str, required=True,
        help='Destination cluster auth URL')
    parser.add_argument(
        '--user', type=str, required=True, help='Destination cluster user')
    parser.add_argument(
        '--key', type=str, required=True, help='Destination cluster key')
    parser.add_argument(
        '--quick-compare', default=False, action='store_true',
        help='List unmigrated containers')
    parser.add_argument(
        '--detail', default=False, action='store_true',
        help='Low level comparison of containers - very slow recommended with'
             ' --container to limit to a single container')
    args = parser.parse_args(sys.argv[1:])

    q = queue.Queue()
    threads = []

    conn = get_conn(args.auth_url, args.user, args.key, args.account)

    if not args.detail and not args.quick_compare:
        parser.print_help()
        return

    if args.quick_compare:
        containers_only(conn)
        return

    for _ in range(args.workers):
        thread = threading.Thread(target=worker, args=(args, q))
        thread.start()
        threads.append(thread)

    if args.container:
        q.put({'account': args.account, 'container': args.container})
    else:
        for container in iterate_listing(conn.get_account):
            if not container:
                break
            if 'swift' not in container.get('content_location', []):
                print(
                    'Container {} with {} objects has not yet been '
                    'migrated'.format(container['name'], container['count']))
                continue
            q.put({'account': args.account,
                   'container': container['name']})

    q.join()
    for _ in threads:
        q.put(None)
    q.join()

    for t in threads:
        t.join()


main()
