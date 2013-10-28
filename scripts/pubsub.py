#!/usr/bin/env python
import sys
import threading
import time

from redis import config
from redis.exceptions import ResponseError
import redis

ready_list = "SUBSCRIBER-READY"
channel_name = "TEST-CHANNEL"
publisher_count = 1
pipeline_flush = 1000
iterations = 10000
suffix_size = 128
suffix = ""

for i in xrange(0, suffix_size):
    suffix += "X"

class RedisThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        op,path = config.select_config(config.find_configs(), 3)
        cfg = config.from_file(path)

        self.rc = redis.Redis(host=cfg['host'], port=int(cfg['port']))

        if cfg.has_key('auth') and (cfg['auth'] is not None):
            self.rc.execute_command('AUTH', cfg['auth'])

class SubscriberThread(RedisThread):
    def __init__(self, publisher_count):
        RedisThread.__init__(self)
        self.publisher_count = publisher_count

    def run(self):
        ps = self.rc.pubsub()
        ps.subscribe([channel_name])

        t = RedisThread()

        for i in xrange(0, self.publisher_count):
            t.rc.rpush(ready_list, "READY")

        message_count = 0
        start = time.time()

        for item in ps.listen():
            if item["type"] == "message":
                #print trim(item["data"])
                message_count += 1
                if item["data"] == "STOP":
                    break

        end = time.time()

        duration = end - start

        print "recieved %d messages in %f seconds" % (message_count, duration)

class PublisherThread(RedisThread):
    def __init__(self, id):
        RedisThread.__init__(self)
        self.id = id

    def run(self):
        i = 1

        print self.id, "waiting for subscriber..."
        self.rc.blpop(ready_list, timeout=10)

        print self.id, "opening a pipeline..."
        p = self.rc.pipeline()

        flush = pipeline_flush

        while(i < iterations):
            message = str(i) + "-" + suffix
            p.publish(channel_name, message)
            i += 1
            flush -= 1

            if flush < 1:
                flush = pipeline_flush
                start = time.time()
                p.execute()
                end = time.time()
                duration = end - start
                print self.id, "pipeline flushed (took %f seconds)" % duration
                #p = self.rc.pipeline()

        start = time.time()
        p.execute()
        end = time.time()
        duration = end - start
        print self.id, "pipeline flushed (took %f seconds)" % duration

def trim(string):
    length = len(string)

    if length > 32:
        return string[0:28] + " ..."

    return string

def main():   
    publishers = []

    for i in xrange(0, publisher_count):
        p = PublisherThread(i)
        p.start()
        publishers.append(p)

    s = SubscriberThread(publisher_count)
    s.start()

    for p in publishers:
        p.join()

    print "publishing", "STOP"
    RedisThread().rc.publish(channel_name, "STOP")

if __name__ == "__main__":
    main()

