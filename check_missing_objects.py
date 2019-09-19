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
                tries = RETRIES + 10
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
    path = '/v1/{}'.format(urllib.parse.quote(account))
    storage_url = urllib.parse.urljoin(conn.url, path)
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
        src_client = get_conn(config.src_auth_url, config.src_user,
                              config.src_key, account)
        dst_client = get_conn(config.dst_auth_url, config.dst_user,
                              config.dst_key, account)

        if not src_client or not dst_client:
            work_queue.task_done()
            continue

        try:
            src_iter = iterate_listing(
                functools.partial(src_client.get_container,
                                  work['container']))

            dst_iter = iterate_listing(
                functools.partial(dst_client.get_container,
                                  work['container']))

            src_entry = next(src_iter)
            try:
                dst_entry = next(dst_iter)
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    print('Missing container {}'.format(work['container']))
                    dst_entry = None
                elif e.http_status == 401:
                    print('Not authorized to access {}'.format(
                        work['container']))
                    raise Exception('Destination unauthorized - skipping')
                else:
                    raise
            while src_entry is not None:
                if dst_entry is None:
                    results.append(src_entry)
                    src_entry = next(src_iter)
                    continue

                if dst_entry['name'] == src_entry['name']:
                    if dst_entry['hash'] != src_entry['hash']:
                        dst_date = parse_last_modified(dst_entry)
                        src_date = parse_last_modified(src_entry)
                        if dst_date <= src_date:
                            results.append(src_entry)
                    dst_entry = next(dst_iter)
                    src_entry = next(src_iter)
                    continue
                if dst_entry['name'] < src_entry['name']:
                    dst_entry = next(dst_iter)
                    continue
                if dst_entry['name'] > src_entry['name']:
                    results.append(src_entry)
                    src_entry = next(src_iter)
                    continue
            if results:
                print('Container {} has {} missing objects:'.format(
                    work['container'], len(results)))
                for entry in results:
                    print(entry['name'])
        except Exception:
            print(traceback.format_exc())
        finally:
            work_queue.task_done()


def retry_head(conn, container):
    i = 0
    hdrs = None
    while i < 3:
        try:
            hdrs = conn.head_container(container)
        except Exception as e:
            print('{} on {} failed: {}'.format(container, conn.url, repr(e)))
        if hdrs:
            hdrs['name'] = container
            return hdrs
        i += 1
    return {'BADRESP': True}


def head_iter(conn):
    h_iter = iterate_listing(conn.get_account)
    entry = next(h_iter)
    while entry:
        hdrs = retry_head(conn, entry['name'])
        yield hdrs
        entry = next(h_iter)

    yield None


def do_heads(src_conn, dst_conn):
    src_iter = head_iter(src_conn)
    dst_iter = head_iter(dst_conn)
    src_entry = next(src_iter)
    dst_entry = next(dst_iter)
    missing_containers = 0
    identical_containers = 0
    extra_containers = 0
    different_containers = 0
    src_failed_count = 0
    while src_entry is not None:
        if src_entry.get('BADRESP'):
            src_failed_count += 1
            src_entry = next(src_iter)
        else:
            if dst_entry is not None and \
                    dst_entry['x-timestamp'] == src_entry['x-timestamp']:
                print('--')
            if dst_entry is None or dst_entry['name'] > src_entry['name'] or \
                    dst_entry.get('BADRESP'):
                print('{}: Source: {} obj {} bytes, Dest: N/A'.format(
                    src_entry['name'],
                    src_entry['x-container-object-count'],
                    src_entry['x-container-bytes-used']))
                src_entry = next(src_iter)
                if dst_entry and dst_entry.get('BADRESP'):
                    dst_entry = next(dst_iter)
                missing_containers += 1
                continue
            if dst_entry['name'] == src_entry['name']:
                print('{}: Source: {} obj {} bytes, Dest: {}'
                      ' obj {} bytes'.format(
                          src_entry['name'],
                          src_entry['x-container-object-count'],
                          src_entry['x-container-bytes-used'],
                          dst_entry['x-container-object-count'],
                          dst_entry['x-container-bytes-used']))
                if src_entry['x-container-object-count'] == \
                        dst_entry['x-container-object-count'] and \
                        src_entry['x-container-bytes-used'] == \
                        dst_entry['x-container-bytes-used']:
                    identical_containers += 1
                else:
                    different_containers += 1
                src_entry = next(src_iter)
                dst_entry = next(dst_iter)
                continue
            extra_containers += 1
            dst_entry = next(dst_iter)
    print("Summary: {} identical containers,\n"
          "         {} different containers\n"
          "         {} missing containers\n"
          "         {} failed source containers\n"
          "         {} extra containers".format(identical_containers,
                                                different_containers,
                                                missing_containers,
                                                src_failed_count,
                                                extra_containers))


def do_quick(src_conn, dst_conn):
    src_iter = iterate_listing(src_conn.get_account)
    dst_iter = iterate_listing(dst_conn.get_account)
    src_entry = next(src_iter)
    dst_entry = next(dst_iter)
    missing_containers = 0
    identical_containers = 0
    extra_containers = 0
    different_containers = 0
    while src_entry is not None:
        if dst_entry is not None and \
                dst_entry['last_modified'] == src_entry['last_modified']:
            print('--')
        if dst_entry is None or dst_entry['name'] > src_entry['name']:
            print('{}: Source: {} obj {} bytes, Dest: N/A'.format(
                src_entry['name'], src_entry['count'], src_entry['bytes']))
            src_entry = next(src_iter)
            missing_containers += 1
            continue
        if dst_entry['name'] == src_entry['name']:
            print('{}: Source: {} obj {} bytes, Dest: {} obj {} bytes'.format(
                src_entry['name'], src_entry['count'], src_entry['bytes'],
                dst_entry['count'], dst_entry['bytes']))
            if src_entry['count'] == dst_entry['count'] and \
                    src_entry['bytes'] == dst_entry['bytes']:
                identical_containers += 1
            else:
                different_containers += 1
            src_entry = next(src_iter)
            dst_entry = next(dst_iter)
            continue
        extra_containers += 1
        dst_entry = next(dst_iter)
    print("Summary: {} identical containers,\n"
          "         {} different containers\n"
          "         {} missing containers\n"
          "         {} extra containers".format(identical_containers,
                                                different_containers,
                                                missing_containers,
                                                extra_containers))


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
        '--src-auth-url', type=str, required=True,
        help='Source cluster auth URL')
    parser.add_argument(
        '--src-user', type=str, required=True, help='Source cluster user')
    parser.add_argument(
        '--src-key', type=str, required=True, help='Source cluster key')
    parser.add_argument(
        '--dst-auth-url', type=str, required=True,
        help='Destination cluster auth URL')
    parser.add_argument(
        '--dst-user', type=str, required=True, help='Destination cluster user')
    parser.add_argument(
        '--dst-key', type=str, required=True, help='Destination cluster key')
    parser.add_argument(
        '--summary', default=False, action='store_true',
        help='Summarize account stats')
    parser.add_argument(
        '--quick_compare', default=False, action='store_true',
        help='Account level comparison of containers - fast')
    parser.add_argument(
        '--heads', default=False, action='store_true',
        help='Container level comparison - heads only')
    parser.add_argument(
        '--detail', default=False, action='store_true',
        help='Low level comparison of containers - very slow recommended with'
             ' --container to limit to a single container')
    args = parser.parse_args(sys.argv[1:])

    q = queue.Queue()
    threads = []

    src_conn = get_conn(args.src_auth_url, args.src_user, args.src_key,
                        args.account)
    dst_conn = get_conn(args.dst_auth_url, args.dst_user, args.dst_key,
                        args.account)
    if args.summary:
        src_stats = src_conn.head_account()
        dst_stats = dst_conn.head_account()
        print("Summary:\n"
              "--------\n"
              "Source:      {} Containers, {} Objects, {} Bytes\n"
              "Source Storage URL: {}\n"
              "Destination: {} Containers, {} Objects, {} Bytes\n"
              "Destination Storage URL: {}\n".format(
                src_stats['x-account-container-count'],
                src_stats['x-account-object-count'],
                src_stats['x-account-bytes-used'],
                src_conn.url,
                dst_stats['x-account-container-count'],
                dst_stats['x-account-object-count'],
                dst_stats['x-account-bytes-used'],
                dst_conn.url,))

    if args.quick_compare:
        do_quick(src_conn, dst_conn)

    if args.heads:
        do_heads(src_conn, dst_conn)

    if args.detail:
        if args.container:
            q.put({'account': args.account, 'container': args.container})
        else:
            for container in iterate_listing(src_conn.get_account):
                if not container:
                    break
                q.put({'account': args.account,
                       'container': container['name']})

        for _ in range(args.workers):
            thread = threading.Thread(target=worker, args=(args, q))
            thread.start()
            threads.append(thread)

        q.join()
        for _ in threads:
            q.put(None)
        q.join()

        for t in threads:
            t.join()

    if not(args.detail or args.summary or args.quick_compare or args.heads):
        parser.print_help()


main()
