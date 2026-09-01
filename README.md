# euclid-spring-boot-starter

Spring Boot auto-configuration for [euclid-jdk](https://github.com/jensvogt/euclid-jdk), the Java
client library for the [Euclid](https://github.com/jensvogt/euclid) access and SQS APIs. Adds an
auto-configured `EuclidEqs` and `EuclidEsm` beans, a `@QueueListener` annotation for declaring
message handlers and a `@BucketListener` annotation for declaring handlers of a bucket's object
events.

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
# optional, defaults to /etc/euclid/euclid_cert.crt if readable
euclid.ca-cert-path=
```

This logs in once (see `EuclidEam#login()` in euclid-jdk) and exposes that session's `EuclidEqs`,
`EuclidEsm` and `EuclidEes` clients as beans, injectable like any other Spring bean.

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
