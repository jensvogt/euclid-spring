# euclid-spring-boot-starter

Spring Boot auto-configuration for [euclid-jdk](https://github.com/jensvogt/euclid-jdk), the Java
client library for the [Euclid](https://github.com/jensvogt/euclid) access and SQS APIs. Adds an
auto-configured `EuclidSqs` bean and a `@QueueListener` annotation for declaring message handlers.

Requires Java 25 and Spring Boot 3.

## Installation

```xml
<dependency>
    <groupId>io.github.jensvogt</groupId>
    <artifactId>euclid-spring-boot-starter</artifactId>
    <version>0.1.0</version>
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

This resolves and caches a session (see `EuclidAccess#login()` in euclid-jdk) and exposes it as an
`EuclidSqs` bean, injectable like any other Spring bean.

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

The queue name is resolved to its ERN at startup via `EuclidSqs#getQueueErn`. Handler methods may
take either a `String` (the message body) or a `de.jensvogt.euclid.dto.sqs.model.Message`. Messages
are deleted from the queue after the handler returns, unless `autoDelete = false`:

```java
@QueueListener(value = "orders", maxMessages = 5, waitTime = 10, autoDelete = false)
public void onMessage(Message message) {
    ...
}
```

Each listener runs on its own daemon polling thread, managed by Spring's lifecycle (started on
context refresh, stopped on context close).
