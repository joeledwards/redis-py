#!/usr/bin/env python
import base64
import os
import random
import re
import readline
from redis import config
import redis
import sys
import time

def trim_quotes(string):
    result = string
    if len(string) < 1:
        pass
    elif string[0] == '"':
        result = string.strip('"')
    elif string[0] == "'":
        result = string.strip("'")
    return result

cfg = config.from_file(config.select_config(config.find_configs()))

try:
    db = redis.Redis(host=cfg['host'], port=int(cfg['port']))
    if cfg.has_key('auth'):
        print db.execute_command('AUTH', cfg['auth'])
except KeyError, ex:
    print "Malformed config key '%s' in %s" % (str(ex), config_file)
    sys.exit(1)
except Exception, ex:
    print "Could not connect to database. Details: %s" % str(ex)
    sys.exit(1)

regex = re.compile(r'''((?:[^ "']|"[^"]*"|'[^']*')+)''')

running = True
while running:
    cmd_str = raw_input("> ")
    cmd_str = cmd_str.strip()
    if cmd_str == "":
        continue

    args = regex.split(cmd_str.strip())[1::2]
    args = map(str.strip, args)
    cmd = args[0]
    arg_count = len(args)
    result = ""

    if cmd.startswith('.'):
        command = cmd[1:]
        if command == 'quit':
            running = False
        elif command == 'help':
            print "Actions:"
            print " .b64s <string> - base64 encodes the string"
            print " .b64u <string> - base64 encodes the string"
            print " .b64 <string> - attempts to perform a base64"
            print "                 decode on the string"
            print "                 (tries standard then url-safe)"
            print " .help - print this help message"
            print " .hkeys <key> - list all fields associated with this key"
            print " .keys [regex] - list all keys matching optional regex"
            print " .now - format the current time"
            print " .quit - exit cli"
            print " .time <timestamp> - format the given timestamp"
            print " +<key> <value> - set entry"
            print " ++<key> <field> <value> - set hash entry"
            print " -<key> <value> - remove entry"
            print " --<key> <field> <value> - remove hash entry"
            print " <key> - print value of this entry"
            print " <key> <field> - print value of this hash entry"
        elif command == 'b64s':
            if arg_count != 2:
                result = "wrong number of arguments for command '%s'" % cmd
            else:
                result = base64.standard_b64encode(args[1])
        elif command == 'b64u':
            if arg_count != 2:
                result = "wrong number of arguments for command '%s'" % cmd
            else:
                result = base64.urlsafe_b64encode(args[1])
        elif command == 'b64':
            if arg_count != 2:
                result = "wrong number of arguments for command '%s'" % cmd
            else:
                try:
                    result = base64.standard_b64decode(args[1])
                except:
                    try:
                        result = base64.urlsafe_b64decode(args[1])
                    except:
                        result = "Could not decode string"
        elif command == 'time':
            if arg_count != 2:
                result = "wrong number of arguments for command '%s'" % cmd
            else:
                try:
                    timestamp = long(args[1]) / 1000.0
                    y,m,d,hr,mn,sc,_,j,_ = time.gmtime(timestamp) 
                    ts = long(timestamp * 1000)
                    ms = ts % 1000
                    result = "%04d-%02d-%02d (%03d) %02d:%02d:%02d.%02d  [%d]" % (y,m,d,j,hr,mn,sc,ms,ts)
                except:
                    result = "invalid argument '%s' for command '%s'" % (args[1], cmd)
        elif command == 'now':
            now = long(time.time() * 1000)
            timestamp = long(now) / 1000.0
            y,m,d,hr,mn,sc,_,j,_ = time.gmtime(timestamp) 
            ts = long(timestamp * 1000)
            ms = ts % 1000
            result = "%04d-%02d-%02d (%03d) %02d:%02d:%02d.%02d  [%d]" % (y,m,d,j,hr,mn,sc,ms,ts)
        elif command == 'keys':
            if arg_count > 2:
                result = "wrong number of arguments for command '%s'" % cmd
            else:
                expr = '*'
                if arg_count > 1:
                    expr = trim_quotes(args[1])
                result = db.keys(expr)
        elif command == 'hkeys':
            if arg_count != 2:
                result = "wrong number of arguments for command '%s'" % cmd
            else:
                key = trim_quotes(args[1])
                try:
                    result = db.hkeys(key)
                except redis.exceptions.ResponseError:
                    result = "key for wrong type"
        else:
            result = "Invalid command '%s'" % command
    elif cmd.startswith('++') and arg_count == 3:
        key = trim_quotes(cmd[2:])
        field = trim_quotes(args[1])
        value = trim_quotes(args[2])
        result = db.hset(key, field, value)

    elif cmd.startswith('+') and arg_count == 2:
        key = trim_quotes(cmd[1:])
        value = trim_quotes(args[1])
        result = db.set(key, value)

    elif cmd.startswith('--') and arg_count == 2:
        key = trim_quotes(cmd[2:])
        field = trim_quotes(args[1])
        value = db.hget(key, field)
        status = db.hdel(key,field)
        result = "deleting ('%s','%s') : '%s' [%s]" % (key, field, value, status)

    elif cmd.startswith('-') and arg_count == 1:
        key = trim_quotes(cmd[1:])
        value = db.get(key)
        status = db.delete(key)
        result = "deleting '%s' : '%s' [%s]" % (key, value, status)

    elif arg_count == 2:
        key = trim_quotes(cmd)
        field = trim_quotes(args[1])
        key_type = db.type(key).lower()
        if key_type != 'hash':
            if key_type == "zset":
                key_type = "sorted set"
            result = "key '%s' represents a %s, not a hash" % key_type
        else:
            result = db.hget(key, field)

    elif arg_count == 1:
        key = trim_quotes(cmd)
        key_type = db.type(key).lower()
        if key_type == "none":
            result = "key '%s' not found" % key
        elif key_type == "string":
            result = db.get(key)
        elif key_type == "list":
            length = db.llen(key)
            result = "list %s" % str(db.lrange(key, 0, length - 1))
        elif key_type == "set":
            result = "set %s" % str(db.smembers(key))
        elif key_type == "zset":
            count = db.zcard(key)
            result = "sorted set %s" % str(db.zrange(key, 0, count - 1))
        elif key_type == "hash":
            result = "hash keys %s" % str(db.hkeys(key))
        else:
            result = "unrecognized type (%s) for key '%s'" % (key_type, key)

    else:
        result = "Invalid command string '%s'" % cmd_str

    print result

