#!/usr/bin/env python
import base64
import os
import random
import re
import readline
from redis import config
from redis.exceptions import ResponseError
import redis
import shlex
import sys
import threading
import time
import traceback

# Helper Functions /*{{{*/
def usage(message=None):
    if message is not None:
        print "E:", message
    print "Usage: ", os.path.basename(sys.argv[0]), "<thread_count> <iterations> [redis_config_index]"
    sys.exit(1)

def trim_quotes(string):
    result = string
    if len(string) < 1:
        pass
    elif string[0] == '"':
        result = string.strip('"')
    elif string[0] == "'":
        result = string.strip("'")
    return result

def format_int(x):
    if type(x) not in [type(0), type(0L)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)
#/*}}}*/

class ConnectionThread(threading.Thread): #/*{{{*/
    def __init__(self, iterations, connection_id, cfg, start_time):
        threading.Thread.__init__(self)

        self.cfg = cfg
        self.conn = None

        self.connection_id = connection_id
        self.iterations = iterations
        self.start_time = start_time

        self.connect_time = 0.0
        self.total_read_ms = 0.0
        self.total_write_ms = 0.0
        self.iteration_count = 0
        self.failed = False

        self.info = None

    def get_connect_time(self):
        return self.connect_time

    def get_average_read_time(self):
        if self.iteration_count < 1:
            return 0.0

        return self.total_read_ms / self.iteration_count

    def get_average_write_time(self):
        if self.iteration_count < 1:
            return 0.0

        return self.total_write_ms / self.iteration_count
    
    def run(self):
        try:
            delay = self.start_time - time.time()

            if delay > 0.0:
                time.sleep(self.start_time - time.time())

            start = time.time()
            thread_start = start

            conn = redis.Redis(host=self.cfg['host'], port=int(self.cfg['port']))
            self.conn = conn

            if self.cfg.has_key('auth') and (self.cfg['auth'] is not None):
                conn.execute_command('AUTH', self.cfg['auth'])

            end = time.time()
            self.connect_time = (end - start) * 1000.0
            
            sys.stdout.write("Connection # %d established in %f ms\n" % (self.connection_id, self.connect_time))

            key = "THREAD-" + str(self.connection_id)

            for i in range (0, self.iterations):
                start = time.time()
                previous_value = conn.execute_command('GET', key)
                end = time.time()
                read_ms = (end - start) * 1000.0
                
                start = time.time()
                conn.execute_command('INCR', key)
                end = time.time()
                write_ms = (end - start) * 1000.0

                #sys.stdout.write("Connection # %d : %s [read %f ms] [write %f ms]\n" % (self.connection_id, previous_value, read_ms, write_ms))

                self.total_read_ms += read_ms
                self.total_write_ms += write_ms
            
                self.iteration_count += 1

        except redis.ConnectionError, e:
            self.failed = True

        thread_end = time.time()

        thread_time = thread_end - thread_start

        fail_msg = ""
        if self.failed:
            fail_msg = "[FAILED]"

        self.info = conn.execute_command('INFO')

        sys.stdout.write("Connection # %d ran for %f ms %s\n" % (self.connection_id, thread_time, fail_msg))
#/*}}}*/

def test_connections(thread_count, iterations, redis_config_index=None): #/*{{{*/
    quit = True

    op,path = config.select_config(config.find_configs(), redis_config_index)

    if op == "quit":
        return quit

    cfg = config.from_file(path)
    again = True
    last_connect = 0.0

    threads = []

    start_time = time.time() + (thread_count / 2000.0)

    all_conn_ms = []
    min_conn_ms = 2.0 ** 31.0
    med_conn_ms = 0.0
    avg_conn_ms = 0.0
    max_conn_ms = 0.0
    total_conn_ms = 0.0

    all_read_ms = []
    min_read_ms = 2.0 ** 31.0
    med_read_ms = 0.0
    avg_read_ms = 0.0
    max_read_ms = 0.0
    total_read_ms = 0.0

    all_write_ms = []
    min_write_ms = 2.0 ** 31.0
    med_write_ms = 0.0
    avg_write_ms = 0.0
    max_write_ms = 0.0
    total_write_ms = 0.0

    failure_count = 0

    try:
        conn_id = 0
        while len(threads) < thread_count:
            thread = ConnectionThread(iterations, conn_id, cfg, start_time)
            threads.append(thread)
            conn_id += 1

        sys.stdout.write("Starting threads...\n")
        for thread in threads:
            thread.start()
        sys.stdout.write("%d threads started.\n" % thread_count)

        while (start_time - time.time()) > 0.25:
            wait = start_time - time.time()
            sys.stdout.write("T -%f\n" % wait)
            time.sleep(0.25)

        sys.stdout.write("Waiting for threads to complete...\n")
        for thread in threads:
            thread.join()

        for thread in threads:
            all_conn_ms.append(thread.get_connect_time())
            total_conn_ms += thread.get_connect_time()
            if thread.get_connect_time() > max_conn_ms:
                max_conn_ms = thread.get_connect_time()
            if thread.get_connect_time() < min_conn_ms:
                min_conn_ms = thread.get_connect_time()

            all_read_ms.append(thread.get_average_read_time())
            total_read_ms += thread.get_average_read_time()
            if thread.get_average_read_time() > max_read_ms:
                max_read_ms = thread.get_average_read_time()
            if thread.get_average_read_time() < min_read_ms:
                min_read_ms = thread.get_average_read_time()

            all_write_ms.append(thread.get_average_write_time())
            total_write_ms += thread.get_average_write_time()
            if thread.get_average_write_time() > max_write_ms:
                max_write_ms = thread.get_average_write_time()
            if thread.get_average_write_time() < min_write_ms:
                min_write_ms = thread.get_average_write_time()

            if thread.failed:
                sys.stdout.write("Connection # %d failed after %d iterations\n" % (thread.connection_id, thread.iteration_count))
                failure_count += 1

        info = threads[0].conn.execute_command('INFO')
        maxKeyLen = max(map(len, info.keys()))
        
        for k,v in sorted(info.items()):
            print str(k + " ").ljust(maxKeyLen + 1, "-"), v


        avg_conn_ms = total_conn_ms / thread_count
        avg_read_ms = total_read_ms / thread_count
        avg_write_ms = total_write_ms / thread_count

        med_conn_ms = sorted(all_conn_ms)[len(all_conn_ms) / 2]
        med_read_ms = sorted(all_read_ms)[len(all_read_ms) / 2]
        med_write_ms = sorted(all_write_ms)[len(all_write_ms) / 2]

        sys.stdout.write("\n%s clients connected\n" % info['connected_clients'])
        sys.stdout.write("\nFailed: %d\n" % failure_count)
        sys.stdout.write("Connect: [min %f ms] [median %f ms] [average %f ms] [max %f ms]\n" % (min_conn_ms, med_conn_ms, avg_conn_ms, max_conn_ms))
        sys.stdout.write("Read:    [min %f ms] [median %f ms] [average %f ms] [max %f ms]\n" % (min_read_ms, med_read_ms, avg_read_ms, max_read_ms))
        sys.stdout.write("Write:   [min %f ms] [median %f ms] [average %f ms] [max %f ms]\n" % (min_write_ms, med_write_ms, avg_write_ms, max_write_ms))

    except KeyError, ex:
        print "Malformed config key '%s' in %s" % (str(ex), path)
        sys.exit(1)
    except redis.ConnectionError, ex:
        print "Connection error", str(ex)
    except ResponseError, ex:
        print "Reconnecting after Response Error: %s" % str(ex)
    except KeyboardInterrupt, ex:
        print
    except Exception, ex:
        print "An unknown error ocurred. Details: %s" % str(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    return quit
#/*}}}*/

def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        usage("wrong number of arguments")

    try:
        thread_count = int(sys.argv[1])
        iterations = int(sys.argv[2])

        if thread_count < 1:
            usage("thread count must be > 0")

        if iterations < 1:
            usage("iteration count must be > 0")

        redis_config = None
        if len(sys.argv) > 3:
            redis_config = int(sys.argv[3])
    except ValueError, e:
        usage("arguments must be integer values")

    test_connections(thread_count, iterations, redis_config)

if __name__ == "__main__":
    main()

    #sys.exit(0)
