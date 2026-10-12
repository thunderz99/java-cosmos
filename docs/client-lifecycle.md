# Client lifecycle and graceful shutdown

All three database implementations (Cosmos DB, MongoDB, PostgreSQL) register a JVM
shutdown hook by default. Existing constructors keep this behavior.

Applications that control their own shutdown sequence can disable this hook:

```java
Cosmos cosmos = new CosmosBuilder()
        .withDatabaseType(dbType)
        .withConnectionString(connectionString)
        .withRegisterShutdownHook(false)
        .build();

// Stop admission and finish database-dependent tasks, cleanup and status writes.
cosmos.closeClient();
```

The application must retain and close every client it creates. Close an account
only after all users of its database handles have finished. Disabling the hook
transfers responsibility for closing the client to the application; it does not
drain database operations or wait for business tasks.

`closeClient()` and the automatic hook share the same closing logic:

- Concurrent calls wait for the current close attempt to finish.
- Once closing succeeds, further calls do nothing.
- Closing failures are logged and propagated to explicit callers. A subsequent
  call, including the automatic hook, can retry. The resource may already be
  partially closed, so a retry does not guarantee recovery.
- Successful explicit closing removes the hook during normal execution. During
  JVM shutdown, hook removal is prohibited; the already-closed resource remains
  protected against duplicate closing.
- Initialization failures after resource creation trigger cleanup. If cleanup
  also fails, its exception is suppressed on the original initialization failure.

JVM hooks have no defined execution order and may run concurrently. Applications
must put database closing into their own ordered shutdown sequence instead of
relying on the order in which hooks were registered. Timeout policies and the
handling of unfinished tasks remain application responsibilities.
