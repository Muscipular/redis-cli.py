#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import argparse, sys, os
import json
import re
import time

import redis


def props(obj):
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if not name.startswith('__') and not callable(value):
            pr[name] = value
    return pr


class Host:
    def __init__(self):
        self.unix_socket_path = None
        self.host = None
        self.port = '6379'
        self.ssl = False
        # cluster = False,
        self.db = 0
        self.password = None


# redis.StrictRedis.RESPONSE_CALLBACKS.clear()
redis.StrictRedis.RESPONSE_CALLBACKS.pop("PING")
redis.StrictRedis.RESPONSE_CALLBACKS.pop("INFO")

un_escape = lambda e: "(nil)" if e is None else e if not isinstance(e, str) else '"{0}"'.format(str(e).replace("\\", "\\\\").replace("\"", "\\\""))
is_repl = False
args = None
host = None


def cmd_parse(s):
    ctx, result = {}, []
    UNKOWN, QOUTE, UNQOUTE = 0, 1, 2

    def reset():
        ctx['n'] = ''
        ctx['ch'] = ''
        ctx['escape'] = False
        ctx['qoute'] = UNKOWN

    def append(force=False):
        if ctx['n'] or force: result.append(ctx['n'])
        reset()

    reset()
    for v in s:
        if ctx['escape']:
            ctx['n'] += v
            ctx['escape'] = False
            continue
        if v in ['"', "'"]:
            if ctx['ch'] == '' and ctx['qoute'] == UNKOWN:
                ctx['ch'] = v
                ctx['qoute'] = QOUTE
                continue
            elif ctx['ch'] == v and ctx['qoute'] == QOUTE:
                append(True)
                continue
        if v == '\\':
            ctx['escape'] = True
            continue
        if v in [' ', '\t']:
            if ctx['qoute'] == UNQOUTE: append()
            if ctx['qoute'] == UNKOWN: continue
        if ctx['qoute'] == UNKOWN: ctx['qoute'] = UNQOUTE
        ctx['n'] += v

    append()
    return result


def scan_all(client, cmd):
    result, index, count = [], 0, 0
    cmd = list(cmd)
    if len(cmd) == 2:
        cmd.insert(1, "MATCH")
    for i, n in enumerate(cmd):
        if n.upper() == 'COUNT' and len(cmd) > i + 1:
            count = cmd[i + 1]
            cmd[i:i + 2] = []
            break
    if count <= 0:
        size = raw_do(client, ['DBSIZE'])
        if size == 0:
            return []
        size = int(size / 10) if isinstance(size, int) else 1000
        if size < 1000:
            count = 1000
        elif size > 20000:
            count = 20000
        else:
            count = size
    while True:
        resp = raw_do(client, ['SCAN', index] + cmd[1:] + ['COUNT', count])
        if isinstance(resp, Exception):
            return resp
        if not isinstance(resp, (list, tuple)):
            break
        index = resp[0]
        result = result + list(resp[1])
        if resp[0] == 0:
            break
    return result


def eval_fn(c):
    return lambda client, cmd: raw_do(client, [c] + list(cmd)[1:])


def del_all(client, cmd):
    if len(cmd) != 2 or cmd[1] == '':
        raise redis.exceptions.ResponseError('Argument error')
    data = scan_all(client, ['', "MATCH", cmd[1]])
    if isinstance(data, Exception):
        return data
    result = 0
    step = 1000
    for i in range(0, len(data), step):
        batch_data = data[i:i + step]
        if args.cluster:  # MDEL command not work in cluster mode with different solt, use lua script instead
            script = '''
            local c = 0;
            for i, v in ipairs(ARGV) do
                redis.call("DEL", v); 
                c = c + 1; 
            end 
            return c;
            '''
            r = raw_do(client, ["EVAL", script, '0'] + batch_data)
        else:
            r = raw_do(client, ["DEL"] + batch_data)
        if isinstance(r, Exception):
            log_debug(r)
            break
        result = result + r
    return [result, data]


def get_match(client, cmd):
    keys = scan_all(client, ['', "MATCH", cmd[1]])
    if isinstance(keys, Exception):
        return keys
    result = []
    step = 100
    for i in range(0, len(keys), step):
        batch_data = keys[i:i + step]
        if args.cluster:  # MGET command not work in cluster mode with different solt, use lua script instead
            script = '''
            local c = {};
            for i, v in ipairs(ARGV) do
                table.insert(c, redis.call("GET", v));
            end 
            return c;
            '''
            ret = raw_do(client, ["EVAL", script, '0'] + batch_data)
        else:
            ret = raw_do(client, ["MGET"] + batch_data)
        if isinstance(ret, Exception):
            return ret
        result = result + ret
    map = {}
    for i, key in enumerate(keys):
        map[key] = result[i]
    return map


custom_cmd = {
    'QUIT': lambda _, __: sys.exit(0),
    'EXIT': lambda _, __: sys.exit(0),
    'SCANALL': scan_all,
    "EVALALL": eval_fn('EVAL'),
    "EVALSHAALL": eval_fn('EVALSHA'),
    "DELALL": del_all,
    "GETMATCH": get_match,
}


def do(client, cmd):
    log_debug("do:", cmd)
    if not isinstance(cmd, list):
        n = cmd_parse(cmd)
        cmd = n  # list(filter(lambda c: c, cmd.split(' ')))
    cmd[0] = cmd[0].upper()
    cmd, fmt = extract_format(cmd)
    if should_do_in_cluster(cmd[0]):
        resp = cluster_do(client, cmd)
        if resp is None:
            return
    else:
        resp = raw_do(client, cmd)
    if not args.cluster and cmd[0] == "SELECT" and len(cmd) == 2 and re.match('^\d+$', cmd[1]) and resp == True:
        client.connection_pool.connection_kwargs['db'] = host.db = int(cmd[1])
    if cmd[0] == "INFO":
        indent_print(resp)
    else:
        fmt.format(resp)


def should_do_in_cluster(cmd):
    return args.cluster and cmd in ['KEYS', "FLUSHDB", "FLUSHALL", 'SCANALL', "EVALALL", "EVALSHAALL", 'DELALL', 'GETMATCH']


class Formatter(object):
    def format(self, data):
        raise NotImplementedError("format is not implemented.")


class SubFormatter(Formatter):
    def format(self, data):
        raise NotImplementedError("format is not implemented.")


class ResponseFormatter(Formatter):
    def format(self, data):
        raise NotImplementedError("format is not implemented.")

    def __init__(self, sub_formatter=None) -> None:
        super().__init__()
        self.sub_formatter = sub_formatter


class RedisCliResponseFormatter(ResponseFormatter):
    def __init__(self, sub_formatter=None) -> None:
        super().__init__(sub_formatter)

    def format(self, data):
        self.printAsRedisCli(data)

    def printAsRedisCli(self, resp, den=0):
        fmt = self.sub_formatter
        if resp is None:
            indent_print("(nil)", den)
            return
        if isinstance(resp, bool):
            indent_print("true" if resp else "false", den)
            return
        if isinstance(resp, str):
            indent_print(un_escape(resp) if fmt is None else fmt.format(resp), den)
            return
        if isinstance(resp, (float, int)):
            indent_print("({0}) {1}".format(type(resp).__name__, resp), den)
            return
        if isinstance(resp, Exception):
            indent_print(format(get_error_text(resp)), den)
            return
        if isinstance(resp, dict):
            if len(resp) == 0:
                indent_print("(empty)", den)
            try:
                for k, v in resp.items():
                    indent_print('\x1b[32m{0}\x1b[0m : {1}'.format(k, un_escape(v) if fmt is None else fmt.format(v)), den)
                return
            except Exception as e:
                log_debug(e)
                log_debug(resp)
                raise e
        if isinstance(resp, (list, tuple)):
            try:
                padding = 0
                if den > 0: padding = 1
                if not resp:
                    indent_print("(empty)")
                    return
                count = get_len_of_len_str(resp)
                for i, value in enumerate(resp[padding:]):
                    current_index = i + padding + 1
                    prefix = pad(" ", count - get_len_of_str(current_index))
                    if not isinstance(value, (list, tuple)):
                        indent_print("{c}{i}) {d}".format(i=current_index, d=un_escape(value) if fmt is None else fmt.format(value), c=prefix), den)
                        continue
                    if len(value) == 0:
                        indent_print("{c}{i}) {d}".format(i=current_index, d='(empty)', c=prefix), den)
                        continue
                    indent_print("{c}{i}){p}1) {d}".format(i=current_index, d=un_escape(value[0]) if fmt is None else fmt.format(value[0]), c=prefix, p=pad(' ', get_len_of_len_str(value))), den)
                    self.printAsRedisCli(value, den + count + get_len_of_str(count) + 1)
            except Exception as e:
                log_debug(type(resp), e, resp)
                raise e


class JsonResponseFormatter(ResponseFormatter):
    def __init__(self, sub_formatter=None, indent=True) -> None:
        super().__init__(sub_formatter)
        self.indent = indent

    def format(self, data):
        if isinstance(data, Exception):
            indent_print(get_error_text(data))
            return
        indent_print(json.dumps(data, ensure_ascii=False, indent=self.indent, sort_keys=True))


class JsonParseFormatter(SubFormatter):
    def __init__(self, indent=True) -> None:
        super().__init__()
        self.indent = indent

    def format(self, data):
        if data is None:
            return 'null'
        if not isinstance(data, str):
            return un_escape(data)
        try:
            return json.dumps(json.loads(data), indent=self.indent, ensure_ascii=False, sort_keys=True)
        except json.decoder.JSONDecodeError:
            return un_escape(data)


def extract_format(cmd):
    fmt, name = None, None
    if len(cmd) > 2 and cmd[-2].upper() == 'AS':
        name = cmd[-1].upper()
        if name in ["JSON", "JsonItem".upper()]:
            fmt = RedisCliResponseFormatter(JsonParseFormatter())
        elif name in ["JsonResponse".upper()]:
            fmt = JsonResponseFormatter(JsonParseFormatter())
        cmd = cmd[0:-2]
    return cmd, fmt or RedisCliResponseFormatter()


def cluster_do(client, cmd):
    try:
        nodes = client.execute_command('cluster', 'nodes').split('\n')
    except (redis.ResponseError, redis.exceptions.ResponseError) as e:
        log_debug(cluster_do.__name__, e)
        return e
    log_debug(nodes)
    nodes = map(lambda c: c.groups(), filter(lambda c: c, map(lambda c: re.match('\w+ (\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}):(\d+) .*master', c), nodes)))
    result = None
    for node in nodes:
        h = props(host)
        h['host'], h['port'] = node
        cc = redis.StrictRedis(decode_responses=True, **h)
        ret = raw_do(cc, cmd)
        if isinstance(ret, Exception):
            print(format_redis_node(node), "response:", get_error_text(ret))
            continue
        if cmd[0] == 'DELALL':
            result = ret if not result else [result[0] + ret[0], result[1] + ret[1]]
        if cmd[0] == "GETMATCH":
            if result is None:
                result = ret
            else:
                result.update(ret)
        elif cmd[0].upper() not in ["KEYS", "SCANALL"]:
            result = (result or []) + [format_redis_node(node), [ret]]
        else:
            result = (result or []) + (ret or [])
    return result


def format_redis_node(node):
    return '[\x1b[32m{0}\x1b[0m:\x1b[33m{1}\x1b[0m]'.format(*node)


def raw_do(client, cmd):
    log_debug("{host}:{port}[{db}]".format(**client.connection_pool.connection_kwargs), "raw_do:", cmd)
    try:
        return (custom_cmd.get(cmd[0]) or (lambda _, __: client.execute_command(*cmd)))(client, cmd)
    except (redis.exceptions.ConnectionError, redis.ConnectionError) as e:
        print(e)
        sys.exit(0)
    except (redis.exceptions.ResponseError, redis.ResponseError) as e:
        v = e.args[0]
        match = re.match("^(?:MOVED|ASK) \d+ (.+):(\d+)", v)
        if args.cluster:
            if match:
                print(v)
                h = props(host)
                h['host'] = match.group(1)
                h['port'] = match.group(2)
                c = redis.StrictRedis(decode_responses=True, **h)
                return raw_do(c, cmd)
        log_debug(e)
        return e
    except Exception as e:
        log_debug(e)
        return e


def log_debug(*arg_list):
    if args.debug:
        print('\x1b[32m[DEBUG]\x1b[0m', *arg_list)


def main():
    global args, host
    args = parse_args()

    if not args:
        sys.exit(0)
    if args.clean_history:
        # noinspection PyBroadException
        try:
            os.remove(get_history_path())
        except:
            pass
        sys.exit(0)

    if args.raw_string:
        global un_escape
        un_escape = lambda s: "(nil)" if s is None else s
    host = Host()
    host.db = args.db
    host.password = args.password
    host.ssl = args.ssl
    if host.unix_socket_path:
        host.unix_socket_path = args.socket
    else:
        host.host = args.host
        host.port = args.port

    client = redis.StrictRedis(decode_responses=True, **props(host))

    if not args.cmd and not args.eval:
        return repl(client)
    cmd = ([args.cmd] if args.cmd else []) + args.args
    if args.eval:
        ok, res = read_file(args.eval)
        if not ok:
            print(res)
            return
        keys, arg = extract_key_and_args(cmd)
        cmd = ["EVAL", res, len(keys)] + keys + arg
    cli_do(client, cmd)


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    argument_group = parser.add_argument_group("connection")
    argument_group.add_argument("-h", "--host", type=str, metavar="host", help="host", dest="host", default='127.0.0.1')
    argument_group.add_argument("-p", "--port", type=int, metavar="port", help="port", dest="port", default=6379)
    argument_group.add_argument("-s", '--socket', type=str, metavar="socket", help="unix socket", dest="socket", default=None)
    argument_group.add_argument("-n", "--database", type=int, metavar="db", help="select db", dest="db", default=0)
    argument_group.add_argument("-a", '--password', type=str, metavar="passwd", help="redis password", dest="password")
    argument_group.add_argument("--ssl", help="enable ssl", action='store_true', dest="ssl")
    argument_group.add_argument("-c", '--cluster-mode', help="enable cluster mode", action='store_true', dest="cluster")
    argument_group = parser.add_argument_group("ext")
    argument_group.add_argument("--raw", "--raw-string", help="display string as raw string", action='store_true', dest="raw_string")
    argument_group.add_argument("--eval", help="Send an EVAL command using the Lua script at <file>.\n (Note: when using --eval the comma separates KEYS[] from ARGV[] items)", type=str, metavar="file [KEYS ...] [, [ARGS ...]]", dest="eval")
    argument_group.add_argument("-r", '--repeat', help="Execute specified command N times", type=int, metavar="N", dest="repeat", default=1)
    argument_group.add_argument("-d", '--delay', help="delay time in seconds,(float)", type=float, metavar="N", dest="delay", default=0)
    parser.add_argument("cmd", nargs="?", type=str, help="command")
    parser.add_argument("args", nargs="*", type=str, help="arguments")
    argument_group = parser.add_argument_group("info")
    argument_group.add_argument("--help", action='store_true', help="show help")
    argument_group.add_argument("--debug", action='store_true', help="debug mode")
    argument_group.add_argument("--no-autocomplete", action='store_true', help="disable autocomplete", dest="no_autocomplete")
    argument_group.add_argument("--clean-history", action='store_true', help="clean history", dest="clean_history")
    argument_group.add_argument("--no-history", action='store_true', help="disable history", dest="no_history")
    argv = parser.parse_args()

    if argv.help:
        parser.print_help()
        return None
    return argv


def cli_do(client, cmd):
    if args.delay: time.sleep(args.delay)
    do(client, cmd)
    if args.repeat <= 1:
        return
    for i in range(args.repeat - 1):
        if args.delay: time.sleep(args.delay)
        do(client, cmd)


def read_file(path):
    if not os.path.exists(path):
        return False, '(error) file "{0}" not found.'.format(args.eval)
    try:
        file = open(args.eval)
        lua = file.read()
        file.close()
        return True, lua
    except IOError as e:
        return False, str(e)


def extract_key_and_args(c):
    if not ',' in c:
        return c, []
    return c[:c.index(',')], c[c.index(',') + 1:]


def get_history_path():
    return os.environ["USERPROFILE" if sys.platform == 'win32' else 'HOME'] + "/.redis_cli.py.history"


def indent_print(s, den=0):
    print("{0}{1}".format(' ' * den, s))


def get_error_text(t): return (str(type(t)) if not isinstance(t, (redis.exceptions.ResponseError, redis.ResponseError)) else "(error)") + ' ' + str(t)


def get_len_of_len_str(a): return get_len_of_str(len(a))


def get_len_of_str(a): return len(str(a))


def pad(c, i): return c * i;


def repl(client):
    global is_repl
    is_repl = True
    try:
        client.ping()
    except Exception as e:
        print(e)
        sys.exit(0)
    from prompt_toolkit import prompt
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.completion import WordCompleter

    keys = ["APPEND", "AUTH", "BGREWRITEAOF", "BGSAVE", "BITCOUNT", "BITFIELD", "BITOP", "BITPOS", "BLPOP", "BRPOP", "BRPOPLPUSH", "CLIENT KILL", "CLIENT LIST", "CLIENT GETNAME", "CLIENT PAUSE", "CLIENT REPLY", "CLIENT SETNAME", "CLUSTER ADDSLOTS", "CLUSTER COUNT-FAILURE-REPORTS",
            "CLUSTER COUNTKEYSINSLOT", "CLUSTER DELSLOTS", "CLUSTER FAILOVER", "CLUSTER FORGET", "CLUSTER GETKEYSINSLOT", "CLUSTER INFO", "CLUSTER KEYSLOT", "CLUSTER MEET", "CLUSTER NODES", "CLUSTER REPLICATE", "CLUSTER RESET", "CLUSTER SAVECONFIG", "CLUSTER SET-CONFIG-EPOCH", "CLUSTER SETSLOT",
            "CLUSTER SLAVES", "CLUSTER SLOTS", "COMMAND", "COMMAND COUNT", "COMMAND GETKEYS", "COMMAND INFO", "CONFIG GET", "CONFIG REWRITE", "CONFIG SET", "CONFIG RESETSTAT", "DBSIZE", "DEBUG OBJECT", "DEBUG SEGFAULT", "DECR", "DECRBY", "DEL", "DISCARD", "DUMP", "ECHO", "EVAL", "EVALSHA", "EXEC",
            "EXISTS", "EXPIRE", "EXPIREAT", "FLUSHALL", "FLUSHDB", "GEOADD", "GEOHASH", "GEOPOS", "GEODIST", "GEORADIUS", "GEORADIUSBYMEMBER", "GET", "GETBIT", "GETRANGE", "GETSET", "HDEL", "HEXISTS", "HGET", "HGETALL", "HINCRBY", "HINCRBYFLOAT", "HKEYS", "HLEN", "HMGET", "HMSET", "HSET", "HSETNX",
            "HSTRLEN", "HVALS", "INCR", "INCRBY", "INCRBYFLOAT", "INFO", "KEYS", "LASTSAVE", "LINDEX", "LINSERT", "LLEN", "LPOP", "LPUSH", "LPUSHX", "LRANGE", "LREM", "LSET", "LTRIM", "MEMORY DOCTOR", "MEMORY HELP", "MEMORY MALLOC-STATS", "MEMORY PURGE", "MEMORY STATS", "MEMORY USAGE", "MGET",
            "MIGRATE", "MONITOR", "MOVE", "MSET", "MSETNX", "MULTI", "OBJECT", "PERSIST", "PEXPIRE", "PEXPIREAT", "PFADD", "PFCOUNT", "PFMERGE", "PING", "PSETEX", "PSUBSCRIBE", "PUBSUB", "PTTL", "PUBLISH", "PUNSUBSCRIBE", "QUIT", "RANDOMKEY", "READONLY", "READWRITE", "RENAME", "RENAMENX",
            "RESTORE", "ROLE", "RPOP", "RPOPLPUSH", "RPUSH", "RPUSHX", "SADD", "SAVE", "SCARD", "SCRIPT DEBUG", "SCRIPT EXISTS", "SCRIPT FLUSH", "SCRIPT KILL", "SCRIPT LOAD", "SDIFF", "SDIFFSTORE", "SELECT", "SET", "SETBIT", "SETEX", "SETNX", "SETRANGE", "SHUTDOWN", "SINTER", "SINTERSTORE",
            "SISMEMBER", "SLAVEOF", "SLOWLOG", "SMEMBERS", "SMOVE", "SORT", "SPOP", "SRANDMEMBER", "SREM", "STRLEN", "SUBSCRIBE", "SUNION", "SUNIONSTORE", "SWAPDB", "SYNC", "TIME", "TOUCH", "TTL", "TYPE", "UNSUBSCRIBE", "UNLINK", "UNWATCH", "WAIT", "WATCH", "ZADD", "ZCARD", "ZCOUNT", "ZINCRBY",
            "ZINTERSTORE", "ZLEXCOUNT", "ZRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZRANGEBYSCORE", "ZRANK", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZREVRANGE", "ZREVRANGEBYSCORE", "ZREVRANK", "ZSCORE", "ZUNIONSTORE", "SCAN", "SSCAN", "HSCAN", "ZSCAN",
            "EXIT", "SCANALL", "EVALALL", "EVALSHAALL", "DELALL", "GETMATCH"]
    r_completer = WordCompleter(keys, ignore_case=True, sentence=True) if not args.no_autocomplete else None
    history = FileHistory(get_history_path()) if not args.no_history else None
    while True:
        try:
            msg = prompt('{host}:{port}[{db}]>'.format(**props(host)), history=history, completer=r_completer)
            if msg: do(client, msg);
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
