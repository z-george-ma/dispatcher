# Dispatcher
-----
### What is Dispatcher?

Dispatcher is a lightweight HTTP based async messaging service, similar to queuing system. It has the following features:

1. Reliability - messages are persisted on file system to prevent message loss
2. At least once delivery
3. Built-in retry mechanism
4. Automatic recovery on start

-----
### How does it work?

Imagine you have two apis, named A and B. A wants to fire and forget an async request to B, but needs to guarantee the message delivery to B. Instead of A sending the request directly to B, Dispatcher works as a delegate to the request, and will automatically retry in the case of failure.

When Dispatcher receives a new message, it first persists it to transaction logs on the disk, and return 202 Accepted to the caller. Then it tries to make the HTTP call to the endpoint specified in the message, and will retry once the call fails.

-----
### Auto recovery

For whatever reason Dispatcher dies, the messages won't be lost as they are persisted in the transaction logs. On the restart, Dispatcher will read the undelivered messages from the transaction logs and resend them.

-----
### Comparison to queues

Consider use of Dispatcher if you need to

- Fire and forget
- Guarantee message delivery
- Retry on failure

For simple use cases, Dispatcher has the following advantages over queues:

- No extra infrastructure needed
- Standard HTTP calls
- No need for worker processes

Consider use of queues if you need to

- Guarantee the order of delivery
- Pull messages rather than push
- The consumer needs to control the rate of reading messages

-----
### Scalability

Dispatcher is a fully self-contained API hence can work independently when hosted behind a load balancer. With current implementation, you need to manually restart faulty Dispatcher node, or specify "--restart=true" if you are using docker container.

-----
### Examples

Assume Dispatcher is hosted on http://localhost/, the following command will tell Dispatcher to make a HTTP call to http://my-url/. If the call fails, Dispatcher will retry initially in 2 seconds, then 4 seconds, 8 seconds, 16 seconds, so on so forth.

```
$ curl http://localhost/ -XPOST -H "Content-Type: application/json" -d "
{
  \"Url\": \"http://my-url/\",
  \"Method\": \"POST\",
  \"Body\": \"{}\",
  \"Header\": {
    \"Content-Type\": \"application/json\"
  },
  \"Timeout\": 1000
}"
```

-----
### Configuration

Dispatcher reads configuration from environment variables. Currently it supports: 

- TRANSACTION_LOG: the name of transaction log file. Default: transaction.log
- LISTEN: the address that Dispatcher will listen to. Default: :80
- WORKER: number of worker threads for Dispatcher. Default: 10
- RETRYLIMIT: The number of retries before the message goes to dead letter queue. Default: 10
- DEFAULT_TIMEOUT: Default timeout for HTTP call if the timeout is not set in the message. Default: 10000

-----
### Installation:

```
$ export GOPATH=`pwd`
$ go test dispatcher   # Run tests
$ go build dispatcher  # Build Dispatcher
$ ./dispatcher         # Start Dispatcher
```

