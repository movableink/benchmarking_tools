Usage:
```
  ./redis-loadtest [OPTIONS] [COMMAND] [KEY]
  -f="": Source Data Filename (required)
  -h=":6379": Redis host
  -t=1: Number of threads
```

Example:
```
./redis-loadtest -t 4 -f messages.json -h some-remote-server.com:4567 rpush clerk_data
```
