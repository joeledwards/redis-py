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
        if cfg['auth'] is not None:
            print self.db.execute_command('AUTH', cfg['auth'])

    def redis(self):
        return self.db

    def remove_keys(self, keys):
        removed = 0
        failed = 0
        for key in keys:
            key_type = self.db.type(key).lower()
            deleted = True
            remove_count = 1
            failure_count = 0
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
                fields = self.db.hkeys(key)
                value = str(len(fields)) + " fields"
                status,remove_count,failed_count = self.remove_hash(key, fields)
            else:
                result = "key '%s' type %s not supported" % (key, key_type)
                deleted = False
                failure_count = 1
                remove_count = 0

            if deleted:
                valueStr = str(value)
                if (len(valueStr) > 64):
                    valueStr = valueStr[:60] + " ..."
                statusStr = str(status)
                if (len(statusStr) > 32):
                    statusStr = statusStr[:28] + " ..."
                print "  deleting (result:%s) : [%s] %s> %s" % (statusStr, key, key_type, valueStr)
                removed += remove_count
            else:
                failed += failure_count

        return "deleted %d of %d keys (%d failures)" % (removed, len(keys), failed)

    def remove_hash(self, key, fields):
        removed = 1
        failed = 0
        for field in fields:
            value = self.db.hget(key,field)
            removed = self.db.hdel(key, field)
            if removed > 0:
                fieldStr = str(field)
                if (len(fieldStr) > 32):
                    fieldStr = fieldStr[:30] + " ..."
                valueStr = str(field)
                if (len(valueStr) > 64):
                    valueStr = valueStr[:60] + " ..."
                print "    deleted [%s : %s] hash-field > %s" % (key, fieldStr, valueStr)
            else:
                failed += 1
                print "    retained [%s : %s] hash-field" % (key, field)
        removed = 1
        if (failed > 0):
            removed = 0
        status = removed
        return (status,removed,failed)

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
        self.working_path = None

        self.__add_action(Action("b64s", self.b64_encode).with_num_args(1).with_usage("<string> - base64 encodes the string (standard)"))
        self.__add_action(Action("b64u", self.b64_encode_url_safe).with_num_args(1).with_usage("<string> - base64 encodes the string (url-safe)"))
        self.__add_action(Action("b64", self.b64_decode).with_num_args(1).with_usage("<string> - attempts to perform a base64 decode on the string (tries standard then url-safe)"))
        self.__add_action(Action("cd", self.cd).with_min_args(0).with_max_args(1).with_usage("[key] - switch the \"working path\" to this prefix"))
        self.__add_action(Action("clean", self.clean).with_min_args(1).with_usage("<key_expr [key_expr ...]> - removes all entries matching the"))
        self.__add_action(Action("close", self.close).with_num_args(0).with_usage("- closes the connection to the database and opens the connection dialog"))
        self.__add_action(Action("del", self.delete).with_num_args(1).with_usage("<key> - remove string entry"))
        self.__add_action(Action("engines", self.list_engines).with_num_args(0).with_usage(" - lists all engines"))
        self.__add_action(Action("engine", self.format_engine).with_num_args(1).with_usage("<engine-id> - prints a custom-formatted summary for this engine if it exists"))
        self.__add_action(Action("entries", self.list_entries).with_min_args(1).with_usage("<key_expr [key_expr ...]> - displays all entries matching the supplied key expressions"))
        self.__add_action(Action("help", self.help).with_num_args(0).with_usage("- print the help message"))
        self.__add_action(Action("hdel", self.hash_delete).with_num_args(2).with_usage("<key> <field> - remove hash entry"))
        self.__add_action(Action("hkeys", self.hash_fields).with_num_args(1).with_usage("- list all fields associated with this hash"))
        self.__add_action(Action("hset", self.hash_set).with_num_args(3).with_usage("<key> <field> <value> - set hash entry"))
        self.__add_action(Action("info", self.info).with_num_args(0).with_usage("- print redis server info"))
        self.__add_action(Action("keys", self.keys).with_min_args(0).with_max_args(1).with_usage("[regex] - list all keys matching optional regex (all keys if no regex supplied)"))
        self.__add_action(Action("ls", self.ls).with_num_args(0).with_usage("- lists keys and unique path-like prefixes of keys"))
        self.__add_action(Action("now", self.now).with_num_args(0).with_usage("- format the current time"))
        self.__add_action(Action("quit", self.quit).with_num_args(0).with_usage("- exit redis CLI"))
        self.__add_action(Action("rename", self.rename).with_num_args(2).with_usage("<key_name> <new_name> - renames a key"))
        self.__add_action(Action("set", self.set).with_num_args(2).with_usage("<key> <value> - set string entry"))
        self.__add_action(Action("show", self.show).with_min_args(1).with_max_args(2).with_usage("<key> [field] - print value of this entry"))
        self.__add_action(Action("time", self.time).with_num_args(1).with_usage("<timestamp> - format the given timestamp"))

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
        key_list = self.db.redis().keys("Engine-*/*")
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
        prefix = "Engine-%s" % id
        key_list = self.db.redis().keys("%s/*" % prefix)
        if len(key_list) < 1:
            return "No engine properties found"
        print "Engine Properties:"
        property_list = map(lambda s: s.split('-', 1)[1], key_list)
        justify = max(map(len, property_list)) + 2
        pairs = zip(property_list, self.db.redis().mget(key_list))
        for k,v in sorted(pairs):
            property_name = k.split('/',1)[1]
            if property_name.startswith("time-"):
                formatted = self.time(v)
            elif property_name.startswith("mem-"):
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
        for k,v in sorted(info.items()):
            print str(k + " ").ljust(maxKeyLen + 1, "-"), v
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

    def rename(self, key_name, new_name):
        key_name = trim_quotes(key_name)
        new_name = trim_quotes(new_name)
        return self.db.redis().rename(key_name, new_name)

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

    def cd(self, path=None):
        if path is not None:
            path = path.rstrip("/") + "/"
            path_match = path + "*"
            matches = self.db.redis().keys(path_match)
            if matches is None or len(matches) < 1:
                return "Path '%s' not found" % path
            else:
                self.working_path = path
        else:
            self.working_path = ""
        return "Working Path: " + self.working_path

    # We need a number of improvements for this capability
    # - Need to summarize keys at the level of the working path, but we need
    #   to print the names of the keys without the working path as a prefix.
    #   Consider updating self.db.display_entries to support paths.
    def ls(self):
        #key = trim_quotes(key)
        if self.working_path is None:
            matches = self.db.redis().keys("*")
        else:
            matches = self.db.redis().keys(self.working_path + "*")
            matches = list(map(lambda s: s.replace(self.working_path, "", 1), matches))
        if matches is None:
            return "no keys found"
        keys = []
        path_map = {}
        for match in matches:
            parts = match.split("/")
            key = parts[0]
            if len(parts) > 1:
                key += "/"
                if not path_map.has_key(key):
                    path_map[key] = 0
                path_map[key] += 1
            else:
                keys.append(key)
        map_summary = ""
        for key in path_map.keys():
            map_summary += "  [%s] PATH> %d\n" % (key, path_map[key])
        return map_summary + self.db.display_entries(keys)

#/*}}}*/

def select_db(): #/*{{{*/
    quit = True

    op,path = config.select_config(config.find_configs())
    if op == "quit":
        return quit

    cfg = config.from_file(path)
    again = True
    last_connect = 0.0
    while again: 
        try:
            connect_time = time.time()
            if connect_time - last_connect < 0.25:
                again = False
                quit = False
                continue

            last_connect = connect_time
            again = False
            db = Database(cfg)
            print "Connected to Redis server:  %(host)s:%(port)s" % cfg

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
