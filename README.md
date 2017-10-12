# redis-copier

redis-copier copy all data between two databases.

# Installation

```bash
$ stack build
```

# Examples

```bash
$ stack exec redis-copier -- --src-host 127.0.0.1 --src-port 6379 --src-db 0 --dst-host 127.0.0.1 --dst-port 6389 --dst-db 0
```
