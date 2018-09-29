# redis-cli.py
simple redis cli client

redis command line interface like redis-cli
support ssl and cluster mode

dependence: python3.6+, redis, prompt_toolkit

# install
```
pip install redis
pip install prompt_toolkit
```

# usage 
like redis-cli, see redis-cli.py --help

# extensions:
1. support print as json, eg:

```
redis-cli.py set a '{"a":1}'
> true
redis-cli.py get a as json
> {
>   "a": 1
> }
```
2. `KEYS` in cluster mode will search all master node

3. `SCANALL` command

    like keys but use `scan` command

    argmuents: SCANALL [MATCH pattern] [COUNT count]

    **COUNT option**
    
    default value is calculate by `DBSIZE` command, min value is 1000, max value is 5000
4. `EVALALL` `EVALSHAALL` eval lua script in all master nodes
5. `DELALL` delete match keys in all master node

    argmuents: DELALL [pattern]

6. `GETMATCH` get match keys
    
    argmuents: GETMATCH [pattern]