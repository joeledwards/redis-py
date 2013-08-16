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

class ConnectionThread(threading.Thread):
	def __init__(self, iterations, connection_id, cfg, start_time):
		threading.Thread.__init__(self)

		self.cfg = cfg
		self.connection_id = connection_id
		self.iterations = iterations
		self.start_time = start_time

		self.connect_time = 0.0
		self.total_read_ms = 0.0
		self.total_write_ms = 0.0
		self.iteration_count = 0.0

	def get_connect_time(self):
		return self.connect_time

	def get_average_read_time(self):
		return self.total_read_ms / self.iteration_count

	def get_average_write_time(self):
		return self.total_write_ms / self.iteration_count
	
	def run(self):
		time.sleep(self.start_time - time.time())

		start = time.time()
		conn = redis.Redis(host=self.cfg['host'], port=int(self.cfg['port']))

		if self.cfg.has_key('auth'):
			conn.execute_command('AUTH', self.cfg['auth'])

		end = time.time()
		self.connect_time = (end - start) * 1000.0

		
		sys.stdout.write("Connection # %d established in %f ms\n" % (self.connection_id, self.connect_time))

		for i in range (0, self.iterations):
			start = time.time()
			previous_value = conn.execute_command('GET', 'COUNT')
			end = time.time()
			read_ms = (end - start) * 1000.0
			
			start = time.time()
			conn.execute_command('INCR', 'COUNT')
			end = time.time()
			write_ms = (end - start) * 1000.0

			#sys.stdout.write("Connection # %d : %s [read %f ms] [write %f ms]\n" % (self.connection_id, previous_value, read_ms, write_ms))

			self.total_read_ms += read_ms
			self.total_write_ms += write_ms
		
			self.iteration_count += 1

def test_connections(thread_count, iterations): #/*{{{*/
	quit = True

	op,path = config.select_config(config.find_configs())

	if op == "quit":
		return quit

	cfg = config.from_file(path)
	again = True
	last_connect = 0.0

	threads = []

	start_time = time.time() + 1.0

	total_conn_ms = 0.0
	total_read_ms = 0.0
	total_write_ms = 0.0

	try:
		conn_id = 0
		while len(threads) < thread_count:
			thread = ConnectionThread(iterations, conn_id, cfg, start_time)
			threads.append(thread)
			conn_id += 1

		for thread in threads:
			thread.start()

		for thread in threads:
			thread.join()

		for thread in threads:
			total_conn_ms += thread.get_connect_time()
			total_read_ms += thread.get_average_read_time()
			total_write_ms += thread.get_average_write_time()

		average_conn_ms = total_conn_ms / thread_count
		average_read_ms = total_read_ms / thread_count
		average_write_ms = total_write_ms / thread_count

		sys.stdout.write("\nAverages: [connect %f ms] [read %f ms] [write %f ms]\n" % (average_conn_ms, average_read_ms, average_write_ms))
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

def usage(message=None):
	if message is not None:
		print "E:", message
	print "Usage: ", os.path.basename(sys.argv[0]), "<thread_count> <iterations>"
	sys.exit(1)

def main():
	if len(sys.argv) != 3:
		usage("wrong number of arguments")

	try:
		thread_count = int(sys.argv[1])
		iterations = int(sys.argv[2])
	except ValueError, e:
		usage("arguments must be integer values")

	test_connections(thread_count, iterations)

if __name__ == "__main__":
	main()

