# Process: run_daemon, ProcessLock, locked_pid

`process.hh` (POSIX, standard library only) is the daemon's process identity. It was ported
unchanged in behavior from Detent's `detent_cli/daemon.cc` (Detent:276bc58d). Exit statuses are the
caller's: the primitives report outcomes and never exit.

## run_daemon

`run_daemon(detach, log_path, run, startup_timeout)` forks before any thread exists. With
`detach` false it runs `run` in place and `ready()` is a no-op. Otherwise:

```text
parent                         child (setsid, stdout/stderr -> log, stdin -> /dev/null)
  pipe(startup)  --fork-->       run(ready)
  poll(read end, timeout)        ready()      writes "R", closes the pipe
  "R"          -> return 0       throw        writes "E<what>" (4096 bytes at most), _exit(1)
  "E<msg>"     -> throw msg      nonzero run  without a ready() throws "daemon startup failed"
  EOF, no "R"  -> throw          exit before ready(): parent throws "daemon exited without
  timeout      -> SIGTERM, grace, SIGKILL; throw "daemon startup timed out"          reporting readiness"
```

The pipe ends are close-on-exec, so a subprocess `run` execs before `ready()` cannot hold the
pipe open and hide the daemon's end; they are also moved above fd 2 so the log redirect cannot
clobber them. At the timeout the parent SIGTERMs the child, waits up to `term_grace` (1 s) for it, then SIGKILLs
and reaps it, so a child that ignores SIGTERM cannot hold the parent. Construct the
`ProcessLock` inside `run`: one taken before the fork stays held by the detached child while the
file names the parent. A `run` that returns
0 without calling `ready()` is ready when it returns. The log must be a regular file owned by the
caller with one link, opened `O_NOFOLLOW`.

## ProcessLock and locked_pid

`ProcessLock(path, system)` creates the runtime directory (0755 when `system`, else 0700, owned by
the caller and not a symlink), opens the pidfile `O_NOFOLLOW` (0644 or 0600, one link, owned by
the caller), takes `flock(LOCK_EX | LOCK_NB)` and writes its PID. When another process holds the
lock `acquired()` is false and the file is untouched; the lock lasts as long as the object.

`locked_pid(path)` names the holder only while the lock is held: it takes the lock itself, so a
stale file (the holder died) or a missing one names no process, as does a held file whose PID is
not yet written or is not a plain positive number.

The tests are `test/process_test.cc` (`task_process`).
