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
import time
import traceback

# Helper Functions /*{{{*/
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

def test_connections(): #/*{{{*/
	quit = True

	op,path = config.select_config(config.find_configs())
	if op == "quit":
		return quit

	cfg = config.from_file(path)
	again = True
	last_connect = 0.0

	connections = []

	count = 0
	max_connections = 1000
	max_iterations = 10
	iteration = 0

	total_conn_ms = 0.0
	total_read_ms = 0.0
	total_write_ms = 0.0

	while True: 
		try:
			if len(connections) < max_connections:
				start = time.time()
				conn = redis.Redis(host=cfg['host'], port=int(cfg['port']))
				if cfg.has_key('auth'):
					print conn.execute_command('AUTH', cfg['auth'])
				end = time.time()

				connect_time = (end - start) * 1000.0
				total_conn_ms += connect_time

				connections.append(conn)

				print "Connection #", len(connections), "established in", connect_time, "ms"

			else:
				conn_num = 0
				for conn in connections:
					start = time.time()
					previous_value = conn.execute_command('GET', 'COUNT')
					end = time.time()
					read_time = (end - start) * 1000.0
					
					start = time.time()
					conn.execute_command('INCR', 'COUNT')
					end = time.time()
					write_time = (end - start) * 1000.0

					print "Connection #", conn_num, ":", previous_value, "[read", read_time, "ms]", "[write", write_time, "ms]"
					conn_num += 1
					count += 1

					total_read_ms += read_time
					total_write_ms += write_time

				iteration += 1

				if iteration > max_iterations:
					average_conn_ms = total_conn_ms / count
					average_read_ms = total_read_ms / count
					average_write_ms = total_write_ms / count
					print
					print "Averages:", "[connect", average_conn_ms, "ms]", "[read", average_read_ms, "ms]", "[write", average_write_ms, "ms]"
					sys.exit(0)

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
	test_connections()

if __name__ == "__main__":
	main()
