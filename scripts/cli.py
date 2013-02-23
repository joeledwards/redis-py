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

class Database: #/*{{{*/
    def __init__(self, cfg):
        self.db = redis.Redis(host=cfg['host'], port=int(cfg['port']))
        if cfg.has_key('auth'):
            print self.db.execute_command('AUTH', cfg['auth'])

    def redis(self):
        return self.db

    def remove_keys(self, keys):
        removed = 0
        failed = 0
        for key in keys:
            key_type = self.db.type(key).lower()
            deleted = True
            if key_type == "none":
                print "  key '%s' not found" % key
                deleted = False
            elif key_type == "string":
                value = self.db.get(key)
                status = self.db.delete(key)
            elif key_type == "list":
                length = self.db.llen(key)
                value = str(self.db.lrange(key, 0, length - 1))
                status = self.db.ltrim(key, 0, 0)
                status = self.db.lpop(key)
            elif key_type == "hash":
                value = sorted(self.db.hkeys(key))
                status = self.db.hdel(key, "*")
            else:
                result = "key '%s' type %s not supported" % (key, key_type)
                deleted = False

            if deleted:
                valueStr = str(value)
                if (len(valueStr) > 64):
                    valueStr = valueStr[:60] + " ..."
                statusStr = str(status)
                if (len(statusStr) > 32):
                    statusStr = statusStr[:28] + " ..."
                print "  deleting (result:%s) : [%s] %s> %s" % (statusStr, key, key_type, valueStr)
                removed += 1
            else:
                failed += 1

        return "deleted %d of %d keys (%d failures)" % (removed, len(keys), failed)

    def display_entries(self, keys):
        for key in keys:
            key = trim_quotes(key)
            key_type = self.db.type(key).lower()
            value = None
            if key_type == "none":
                print "  key '%s' not found" % key
            elif key_type == "string":
                value = str(self.db.get(key))
            elif key_type == "list":
                length = self.db.llen(key)
                value = str(self.db.lrange(key, 0, length - 1))
            elif key_type == "set":
                value = str(self.db.smembers(key))
            elif key_type == "zset":
                count = self.db.zcard(key)
                value = str(self.db.zrange(key, 0, count - 1))
            elif key_type == "hash":
                value = str(sorted(self.db.hkeys(key)))
            else:
                print "  unrecognized type (%s) for key '%s'" % (key_type, key)

            if value is not None:
                print "  [%s] %s> %s" % (key, key_type, value)

        return "evaluated %d keys" % len(keys)

#/*}}}*/

class Action: #/*{{{*/
    def __init__(self, name, method):
        self.name = name
        self.min_args = 0
        self.max_args = sys.maxint
        self.method = method
        self.usage = ""
        self.running = False

    def with_min_args(self, min_args):
        self.min_args = min_args
        if self.max_args < self.min_args:
            self.max_args = self.min_args
        return self

    def with_max_args(self, max_args):
        self.max_args = max_args
        if self.min_args > self.max_args:
            self.min_args = self.max_args
        return self

    def with_num_args(self, num_args):
        self.min_args = num_args
        self.max_args = num_args
        return self

    def with_usage(self, usage):
        self.usage = usage
        return self

    def call(self, *args):
        if len(args) < self.min_args or len(args) > self.max_args:
            return "wrong number of arguments for command '%s'" % self.name
        return self.method(*args)
#/*}}}*/

class CommandLine: #/*{{{*/
    def __init__(self, db):
        self.db = db
        self.actions = {}
        self.done = False
        self.__add_action(Action("b64s", self.b64_encode).with_num_args(1).with_usage("<string> - base64 encodes the string (standard)"))
        self.__add_action(Action("b64u", self.b64_encode_url_safe).with_num_args(1).with_usage("<string> - base64 encodes the string (url-safe)"))
        self.__add_action(Action("b64", self.b64_decode).with_num_args(1).with_usage("<string> - attempts to perform a base64 decode on the string (tries standard then url-safe)"))
        self.__add_action(Action("clean", self.clean).with_min_args(1).with_usage("<key_expr [key_expr ...]> - removes all entries matching the"))
        self.__add_action(Action("close", self.close).with_num_args(0).with_usage("- closes the connection to the database and opens the connection dialog"))
        self.__add_action(Action("engines", self.list_engines).with_num_args(0).with_usage(" - lists all engines"))
        self.__add_action(Action("engine", self.format_engine).with_num_args(1).with_usage("<engine-id> - prints a custom-formatted summary for this engine if it exists"))
        self.__add_action(Action("entries", self.list_entries).with_min_args(1).with_usage("<key_expr [key_expr ...]> - displays all entries matching the supplied key expressions"))
        self.__add_action(Action("help", self.help).with_num_args(0).with_usage("- print the help message"))
        self.__add_action(Action("hkeys", self.hash_fields).with_num_args(1).with_usage("- list all fields associated with this hash"))
        self.__add_action(Action("info", self.info).with_num_args(0).with_usage("- print redis server info"))
        self.__add_action(Action("keys", self.keys).with_min_args(0).with_max_args(1).with_usage("[regex] - list all keys matching optional regex (all keys if no regex supplied)"))
        self.__add_action(Action("now", self.now).with_num_args(0).with_usage("- format the current time"))
        self.__add_action(Action("quit", self.quit).with_num_args(0).with_usage("- exit redis CLI"))
        self.__add_action(Action("time", self.time).with_num_args(1).with_usage("<timestamp> - format the given timestamp"))
        self.__add_action(Action("set", self.set).with_num_args(2).with_usage("<key> <value> - set string entry"))
        self.__add_action(Action("hset", self.hash_set).with_num_args(3).with_usage("<key> <field> <value> - set hash entry"))
        self.__add_action(Action("del", self.delete).with_num_args(1).with_usage("<key> - remove string entry"))
        self.__add_action(Action("hdel", self.hash_delete).with_num_args(2).with_usage("<key> <field> - remove hash entry"))
        self.__add_action(Action("show", self.show).with_min_args(1).with_max_args(2).with_usage("<key> [field] - print value of this entry"))

    def __add_action(self, action):
        self.actions[action.name] = action

    def loop(self):
        self.running = True
        while self.running:
            cmd_str = raw_input("> ")
            cmd_str = cmd_str.strip()
            if cmd_str == "":
                continue

            arg_list = shlex.split(cmd_str)
            command = arg_list[0].strip()
            args = arg_list[1:]
            result = ""
        
            if self.actions.has_key(command):
                result = self.actions[command].call(*args)
            else:
                result = "Invalid command '%s'" % command
            print result

    def b64_encode(self, string):
        return base64.standard_b64encode(string)

    def b64_encode_url_safe(self, string):
        return base64.urlsafe_b64encode(string)

    def b64_decode(self, string):
        try:
            return base64.standard_b64decode(string)
        except:
            try:
                return base64.urlsafe_b64decode(string)
            except:
                return "Could not decode string"

    def fetch_keys(self, *keys):
        key_list = []
        for keyreg in keys:
            keyreg = trim_quotes(keyreg)
            try:
                key_list.extend(self.db.redis().keys(keyreg))
            except redis.exceptions.ResponseError:
                return "error fetching keys matching '%s'" % keyreg
        return sorted(key_list)

    def clean(self, *keys):
        result = self.fetch_keys(*keys)
        if type(result) != list:
            return result
        return self.db.remove_keys(result)

    def list_entries(self, *keys):
        result = self.fetch_keys(*keys)
        if type(result) != list:
            return result
        return self.db.display_entries(result)

    def list_engines(self):
        key_list = self.db.redis().keys("ENGINE-*/*")
        key_map = {}
        print "Engines:"
        for key in key_list:
            parts = key.split('/')
            if len(parts) < 1:
                continue
            parts = parts[0].split('-')
            if len(parts) < 2:
                continue
            id = parts[1]
            if key_map.has_key(id):
                continue
            print "  ", id
            key_map[id] = True
        return "Found %d engine(s)" % len(key_map)

    def format_engine(self, id):
        prefix = "ENGINE-%s" % id
        key_list = self.db.redis().keys("%s/*" % prefix)
        if len(key_list) < 1:
            return "No engine properties found"
        print "Engine Properties:"
        property_list = map(lambda s: s.split('-', 1)[1], key_list)
        justify = max(map(len, property_list)) + 2
        pairs = zip(property_list, self.db.redis().mget(key_list))
        for k,v in sorted(pairs):
            property_name = k.split('/',1)[1]
            if property_name.startswith("TIME_"):
                formatted = self.time(v)
            elif property_name.startswith("MEM"):
                try:
                    v = int(v)
                    formatted = "%s bytes" % format_int(int(v))
                except:
                    formatted = v
            else:
                formatted = v
            print " %s %s" % ((k+" ").ljust(justify, '-'), formatted)
        return ""

    def close(self):
        self.running = False

    def help(self):
        print "Actions:"
        for k in sorted(self.actions.keys()):
            action = self.actions[k]
            print "  %s %s" % (action.name, action.usage)

    def hash_fields(self, key):
        key = trim_quotes(key)
        try:
            return sorted(self.db.redis().hkeys(key))
        except redis.exceptions.ResponseError:
            return "key for wrong type"

    def info(self):
            info = self.db.redis().info()
            maxKeyLen = max(map(len, info.keys()))
            for k,v in info.items():
                print str(k).rjust(maxKeyLen + 1), ":", v
            return "Info Displayed"

    def keys(self, key="*"):
        expr = trim_quotes(key)
        return sorted(self.db.redis().keys(expr))

    def now(self):
        timestamp = long(time.time() * 1000)
        return self.time(str(timestamp))

    def quit(self):
        self.done = True
        self.running = False

    def time(self, timestamp):
        try:
            timestamp = long(timestamp) / 1000.0
            y,m,d,hr,mn,sc,_,j,_ = time.gmtime(timestamp) 
            ts = long(timestamp * 1000)
            ms = ts % 1000
            return "%04d-%02d-%02d %02d:%02d:%02d.%03d +0000  [%d]" % (y,m,d,hr,mn,sc,ms,ts)
        except:
            return "invalid timestamp format : %s" % str(timestamp)

    def set(self, key, value):
        key = trim_quotes(key)
        value = trim_quotes(value)
        return self.db.redis().set(key, value)

    def hash_set(self, key, field, value):
        key = trim_quotes(key)
        field = trim_quotes(field)
        value = trim_quotes(value)
        return self.db.redis().hset(key, field, value)

    def delete(self, key):
        key = trim_quotes(key)
        return self.db.remove_keys([key])

    def hash_delete(self, key, field):
        key = trim_quotes(key)
        field = trim_quotes(field)
        value = self.db.redis().hget(key, field)
        status = self.db.redis().hdel(key,field)
        valueStr = str(value)
        if (len(valueStr) > 64):
            valueStr = valueStr[:60] + " ..."
        statusStr = str(status)
        if (len(statusStr) > 32):
            statusStr = statusStr[:28] + " ..."
        return "deleting (result:%s) : [%s:%s]> %s" % (statusStr, key, field, valueStr)

    def show(self, key, field=None):
        if field is None:
            return self.get(key)
        else:
            return self.hget(key, field)

    def get(self, key):
        key = trim_quotes(key)
        key_type = self.db.redis().type(key).lower()
        return self.db.display_entries([key])

    def hget(self, key, field):
        key = trim_quotes(key)
        field = trim_quotes(field)
        key_type = self.db.redis().type(key).lower()
        if key_type == 'none':
            return "no entry found for key '%s'"
        elif key_type != 'hash':
            if key_type == "zset":
                key_type = "sorted set"
            return "key '%s' represents a %s, not a hash" % (key, key_type)
        else:
            return self.db.redis().hget(key, field)
#/*}}}*/

def select_db(): #/*{{{*/
    quit = True

    op,path = config.select_config(config.find_configs())
    if op == "quit":
        return quit

    cfg = config.from_file(path)
    again = True
    while again: 
        try:
            again = False
            db = Database(cfg)
            cli = CommandLine(db)
            cli.loop()
            quit = cli.done
            del db
            db = None
        except KeyError, ex:
            print "Malformed config key '%s' in %s" % (str(ex), path)
            sys.exit(1)
        except redis.ConnectionError, ex:
            print ex
            quit = False
        except ResponseError, ex:
            print "Reconnecting after Response Error: %s" % str(ex)
            again = True
        except KeyboardInterrupt, ex:
            print
        except Exception, ex:
            print "An unknown error ocurred. Details: %s" % str(ex)
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)
    return quit
#/*}}}*/

def main():
    while not select_db():
        pass

if __name__ == "__main__":
    main()
