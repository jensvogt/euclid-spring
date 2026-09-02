# euclid-spring-boot-starter

Spring Boot auto-configuration for [euclid-jdk](https://github.com/jensvogt/euclid-jdk), the Java
client library for the [Euclid](https://github.com/jensvogt/euclid) access and SQS APIs. Adds an
auto-configured `EuclidEqs`, `EuclidEsm`, `EuclidEes` and `EuclidEns` beans, and annotations for
declaring handlers: `@QueueListener` for the messages of a queue, `@TopicListener` for the messages
published to a topic, and `@BucketListener` for a bucket's object events.

Requires Java 25 and Spring Boot 4.

## Installation

```xml
<dependency>
    <groupId>io.github.jensvogt</groupId>
    <artifactId>euclid-spring-boot-starter</artifactId>
    <version>0.1.3</version>
</dependency>
```

## Configuration

```properties
euclid.base-url=https://euclid.example.com
euclid.username=jens
euclid.password=secret
# optional, the namespace to make active for the session; unscoped when unset
euclid.namespace=development
# optional, defaults to /etc/euclid/euclid_cert.crt if readable
euclid.ca-cert-path=
```

This logs in once (see `EuclidEam#login()` in euclid-jdk) and exposes that session's `EuclidEqs`,
`EuclidEsm`, `EuclidEes` and `EuclidEns` clients as beans, injectable like any other Spring bean.

`euclid.namespace` is applied at login, so every namespace-scoped call those clients make is
restricted to it. It is worth setting as soon as more than one namespace exists: an unscoped
session resolving a bucket or queue *by name* has nothing to tell two of the same name apart with.

As with any Spring Boot property these bind from the environment, so `EUCLID_BASE_URL`,
`EUCLID_USERNAME`, `EUCLID_PASSWORD` and `EUCLID_NAMESPACE` work without naming them in a file.
Prefer that to writing `euclid.username=${EUCLID_USERNAME}` in your own YAML: configuration
properties binding leaves an unresolvable placeholder as its literal text rather than failing, so a
missing or misspelled variable is sent to the server as the string `${EUCLID_USERNAME}` and comes
back as `401 Invalid credentials` instead of saying what was wrong.

### Applications euclid deploys

An application started by euclid itself (EAP) is configured entirely from the environment the
manager gives it, and needs no properties file. It authenticates in one of two ways, neither of
which involves a password:

| The application runs as | euclid provides | How it authenticates |
| --- | --- | --- |
| a named user who may log in | `EUCLID_ACCESS_KEY_ID`, `EUCLID_SECRET_ACCESS_KEY` | signed requests (SigV4) |
| a technical principal | `EUCLID_CREDENTIALS_FILE` | the bearer token in that file |

A technical principal — the `app-<id>` identity euclid creates for the application — has no
password and is given no access key, so the token file is the only thing it can present. The file
is JSON, mode `0600`, and holds the token together with the identity it names:

```json
{"token":"…","expiresAt":"2026-09-02T12:00:00Z","userId":"app-file-copy",
 "accountId":"000000000000","region":"eu-central-1","endpoint":"https://localhost:5566"}
```

The starter picks whichever is present, preferring the access key, and falls back to
`euclid.username`/`euclid.password` only when there is neither. Set `euclid.credentials-file`
yourself to point at such a file outside euclid; otherwise leave all of this alone.

The token expires - an hour by default (`euclid.modules.eap.credentials-ttl-seconds` on the server),
with euclid rewriting the file at half that - so the clients do not keep the one they started with.
Each takes its token from the file per request (`TokenRefreshable` in euclid-jdk), and the file is
re-read whenever its modification time changes, which is exactly when euclid renames a new one into
place. A process that runs for weeks keeps working without restarting or rebuilding its beans. If
the file becomes unreadable the last good token stays in use and a warning is logged, since a token
with time left on it is better than an application that stops.

## `@QueueListener`

```java
@Component
public class OrderEvents {

    @QueueListener("orders")
    public void onMessage(String body) {
        // handle the message body
    }
}
```

The queue name is resolved to its ERN at startup via `EuclidEqs#getQueueErn`. Handler methods may
take a `String` (the message body), a `de.jensvogt.euclid.dto.eqs.model.Message` (the full
envelope), or a type of their own, which the body is deserialized into as JSON. Messages are deleted
from the queue after the handler returns, unless `autoDelete = false`:

```java
@QueueListener(value = "orders", maxMessages = 5, waitTime = 10, autoDelete = false)
public void onMessage(Message message) {
    ...
}
```

`waitTime` is spent inside `EuclidEqs#receiveMessages` waiting for an `eqs.message.sent` websocket
event for this queue, so a message is normally handled as it arrives rather than on the next poll
tick, and the client falls back to plain polling by itself if the gateway has no websocket support.
Keep it above zero — a `waitTime` of zero makes `receiveMessages` return immediately, turning the
listener's loop into a busy one.

## `@TopicListener`

```java
@Component
public class OrderBroadcasts {

    @TopicListener("order-events")
    public void onMessage(String body) {
        // handle the published message
    }
}
```

A topic is not read directly — it fans out to the queues subscribed to it, and that is the only
delivery ENS has. So at startup the container resolves the topic's ERN via `EuclidEns#getTopicErn`,
creates the delivery queue if it does not exist yet, subscribes it to the topic unless a
subscription to that queue is already registered, and from then on receives from that queue exactly
as `@QueueListener` does — websocket wake-up included. Handler methods take the same parameters as a
queue listener: a `String`, a `Message`, or a type of their own.

The queue is what decides fan-out, so it defaults to one per handler,
`<spring.application.name>-<topic>-<method>`. Two instances of one application therefore share a
queue and split the messages between them, while two handlers — or two applications — each subscribe
their own queue and so each receive every message, which is the point of publishing to a topic.
Name it explicitly to share one deliberately, or to receive from a queue somebody else subscribed:

```java
@TopicListener(value = "order-events", queue = "order-audit", maxMessages = 5, waitTime = 10, autoDelete = false)
public void onMessage(Message message) {
    ...
}
```

## `@BucketListener`

Handlers are woken by the gateway over a websocket - one connection for the whole
application - and fall back to long polling when that connection cannot be made, so
no configuration is needed either way.

```java
@Component
public class InvoiceUploads {

    @BucketListener("invoices")
    public void onObject(Event event) {
        // event.eventType() is esm.object.created / .updated / .deleted,
        // event.payload() carries key, prefix, size, contentType, owner, ...
    }
}
```

Events arrive over the Euclid event bus (EES): at startup the container registers a durable
subscription on `esm.object.created`, `esm.object.updated` and `esm.object.deleted`, filtered to the
bucket, then claims events with `EuclidEes#receiveEvents` and acknowledges them once the handler
returns. Because the filter is applied when an event is published, the subscriber only accumulates
the events it asked for; because events are stored per subscriber, nothing is lost while the
application is down and no queue has to exist.

Handler methods may take an `Event` (the full envelope, including event type, id and delivery
attempts), a `Map<String, Object>` (the payload), or a type of their own, which the payload is
converted into naming only the fields it cares about:

```java
public record UploadedObject(String key, String prefix, long size, String owner) {
}

@BucketListener(value = "invoices", prefix = "reports/", eventTypes = "esm.object.created")
public void onReport(UploadedObject object) {
    ...
}
```

`prefix` restricts the events to one "directory" — it matches the prefix the server derives from
the key, so it names a directory rather than any key starting with those characters — and
`directories = true` additionally delivers the zero-byte directory markers an FTP `MKD` leaves
behind, which are filtered out by default.

### Subscriber names and fan-out

The subscriber name events are stored and claimed under decides fan-out, and defaults to
`<spring.application.name>-<bucket>-<method>`. Two instances of one application therefore share a
subscription — they run the same method, and whichever claims an event first handles it — while two
different handlers of the same bucket each receive their own copy. Set `subscriber` explicitly to
have separate handlers, or separate applications, deliberately share one:

```java
@BucketListener(value = "invoices", subscriber = "invoice-import",
                maxEvents = 5, waitTime = 10, visibilityTimeout = 60, autoAck = false)
public void onObject(Event event) {
    ...
}
```

A handler that throws leaves its event unacknowledged, so it is redelivered once the claim's
`visibilityTimeout` runs out rather than being lost.

## Threading

Each listener runs on its own daemon polling thread, managed by Spring's lifecycle (started on
context refresh, stopped on context close).
