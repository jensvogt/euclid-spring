package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.dto.ees.model.Event;
import de.jensvogt.euclid.dto.ens.GetTopicErnResponse;
import de.jensvogt.euclid.dto.ens.ListSubscriptionsResponse;
import de.jensvogt.euclid.dto.ens.model.Subscription;
import de.jensvogt.euclid.dto.eqs.CreateQueueResponse;
import de.jensvogt.euclid.dto.eqs.ListQueueResponse;
import de.jensvogt.euclid.dto.eqs.model.Queue;
import de.jensvogt.euclid.dto.esm.GetBucketErnResponse;
import de.jensvogt.euclid.dto.esm.SubscribeResponse;
import de.jensvogt.euclid.dto.eqs.GetQueueErnResponse;
import de.jensvogt.euclid.dto.eqs.ReceiveMessagesResponse;
import de.jensvogt.euclid.dto.eqs.model.Message;
import de.jensvogt.euclid.exception.EuclidServiceException;
import de.jensvogt.euclid.module.esm.EuclidEsm;
import de.jensvogt.euclid.module.ens.EuclidEns;
import de.jensvogt.euclid.module.eqs.EuclidEqs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EuclidListenerContainerTest {

    /** What a bucket subscription delivers: the notification body, as ESM serialises it. */
    private static final String BUCKET_EVENT_BODY = """
            {"eventType":"esm.object.created","bucketErn":"bucket-ern","key":"reports/q3.csv",\
            "ern":"object-ern","size":4211,"contentType":"text/csv","md5Sum":"abc"}""";

    private static final List<String> OBJECT_EVENTS =
            List.of("esm.object.created", "esm.object.updated", "esm.object.deleted");

    private EuclidEqs euclidEqs;
    private EuclidEsm euclidEsm;
    private EuclidEns euclidEns;
    private EuclidListenerContainer container;

    @BeforeEach
    void setUp() throws Exception {
        euclidEqs = mock(EuclidEqs.class);
        when(euclidEqs.getQueueErn(anyString())).thenReturn(GetQueueErnResponse.builder().ern("test-ern").build());

        euclidEsm = mock(EuclidEsm.class);
        when(euclidEsm.getBucketErn(anyString())).thenReturn(GetBucketErnResponse.builder().ern("bucket-ern").build());
        when(euclidEsm.subscribe(anyString(), anyString(), anyString(), anyList(), anyString(), anyBoolean()))
                .thenReturn(SubscribeResponse.builder().ern("subscription-ern").build());
        when(euclidEqs.createQueue(anyString(), anyLong(), anyLong(), anyLong(), anyString()))
                .thenReturn(CreateQueueResponse.builder().name("delivery").ern("delivery-ern").build());
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString()))
                .thenReturn(ListQueueResponse.builder().queues(Collections.emptyList()).total(0).build());

        euclidEns = mock(EuclidEns.class);
        when(euclidEns.getTopicErn(anyString())).thenReturn(GetTopicErnResponse.builder().ern("topic-ern").build());
        when(euclidEns.listSubscriptions(anyString())).thenReturn(ListSubscriptionsResponse.builder()
                .subscriptions(List.of(new Subscription("sub-ern", "topic-ern", "SQS", "test-ern", "now", "now")))
                .total(1).build());

        @SuppressWarnings("unchecked")
        ObjectProvider<JsonMapper> objectMapperProvider = mock(ObjectProvider.class);
        when(objectMapperProvider.getIfAvailable(any())).thenReturn(new JsonMapper());

        container = new EuclidListenerContainer(providerOf(euclidEqs), providerOf(euclidEsm), providerOf(euclidEns),
                objectMapperProvider);
    }

    @AfterEach
    void tearDown() {
        container.stop();
    }

    @Test
    void deserializesTypedPayloadAndDeletesMessage() throws Exception {
        stubReceive(message("{\"name\":\"abc\",\"value\":5}"));
        TypedHandler handler = new TypedHandler();
        container.register(handler, TypedHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertEquals(new TestPayload("abc", 5), handler.received));
        verify(euclidEqs, timeout(1000)).deleteMessage("receipt-1");
    }

    @Test
    void passesRawBodyForStringParameter() throws Exception {
        stubReceive(message("{\"name\":\"abc\",\"value\":5}"));
        StringHandler handler = new StringHandler();
        container.register(handler, StringHandler.class.getMethod("handle", String.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertEquals("{\"name\":\"abc\",\"value\":5}", handler.received));
    }

    @Test
    void passesFullEnvelopeForMessageParameter() throws Exception {
        Message theMessage = message("body-text");
        stubReceive(theMessage);
        MessageHandler handler = new MessageHandler();
        container.register(handler, MessageHandler.class.getMethod("handle", Message.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertEquals(theMessage, handler.received));
    }

    @Test
    void malformedPayloadIsNotDeletedAndHandlerNotInvoked() throws Exception {
        stubReceive(message("not-json"));
        TypedHandler handler = new TypedHandler();
        container.register(handler, TypedHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertFalse(handler.received != null));
        verify(euclidEqs, never()).deleteMessage(anyString());
    }

    @Test
    void bucketListenerCreatesItsOwnQueueAndSubscribesItFiltered() throws Exception {
        stubReceive(message(BUCKET_EVENT_BODY));
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "reports/", false);

        container.start();

        ArgumentCaptor<String> queueName = ArgumentCaptor.forClass(String.class);
        verify(euclidEqs, timeout(1000)).createQueue(queueName.capture(), eq(300L), anyLong(), anyLong(), eq(""));
        // The run id keeps this run's queue apart from one another run of the same listener owns.
        assertTrue(queueName.getValue().startsWith("invoice-import-"), queueName.getValue());
        verify(euclidEsm, timeout(1000)).subscribe("bucket-ern", "SQS", "delivery-ern", OBJECT_EVENTS, "reports/",
                false);
        verify(euclidEqs, timeout(1000).atLeastOnce()).receiveMessages("delivery-ern", 10, 0);
    }

    @Test
    void mapsTheDeliveredMessageIntoAnEventEnvelope() throws Exception {
        stubReceive(message(BUCKET_EVENT_BODY));
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false);

        container.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertNotNull(handler.received));
        Event event = handler.received;
        assertEquals("esm.object.created", event.eventType());
        // The queue's own two: which delivery this is, and whether it is a redelivery.
        assertEquals("123", event.eventId());
        assertEquals(2, event.attempts());
        assertEquals("reports/q3.csv", event.payload().get("key"));
        verify(euclidEqs, timeout(1000)).deleteMessage("receipt-1");
    }

    @Test
    void convertsTheNotificationToTypedParameter() throws Exception {
        stubReceive(message(BUCKET_EVENT_BODY));
        ObjectHandler handler = new ObjectHandler();
        registerBucket(handler, ObjectHandler.class.getMethod("handle", ObjectSummary.class), "invoices", "", false);

        container.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(
                () -> assertEquals(new ObjectSummary("reports/q3.csv", 4211), handler.received));
    }

    @Test
    void deletesTheQueueAndItsSubscriptionOnShutdown() throws Exception {
        stubReceive(message(BUCKET_EVENT_BODY));
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false);
        container.start();
        verify(euclidEsm, timeout(1000)).subscribe(anyString(), anyString(), anyString(), anyList(), anyString(),
                anyBoolean());

        container.stop();

        // Both, and the subscription first: one outliving the other leaves the server delivering
        // to a queue nothing reads, or a queue nobody will ever drain.
        verify(euclidEsm, timeout(1000)).unsubscribe("subscription-ern");
        verify(euclidEqs, timeout(1000)).deleteQueue("delivery-ern");
    }

    @Test
    void sweepsOnlyTheQueuesWhoseOwnerStoppedSayingItWasAlive() throws Exception {
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString())).thenReturn(
                ListQueueResponse.builder().queues(List.of(
                        queue("invoice-import-dead", "crashed-ern", Instant.now().minusSeconds(3600)),
                        queue("invoice-import-live", "live-ern", Instant.now()))).total(2).build());
        stubReceive(message(BUCKET_EVENT_BODY));
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false);

        container.start();

        verify(euclidEqs, timeout(1000)).deleteQueue("crashed-ern");
        // A live instance's queue looks exactly like a crashed one's but for the heartbeat, so
        // sweeping it would take another listener's deliveries with it.
        verify(euclidEqs, never()).deleteQueue("live-ern");
    }

    /**
     * Deleting the queue is only half of it: the subscription is what the server fans an event out
     * to, so one left pointing at a deleted queue makes every upload into the bucket produce a
     * "target queue not found" for ever, and there is one more of them after every restart.
     */
    @Test
    void sweepsTheSubscriptionsOfQueuesThatAreGone() throws Exception {
        String deadErn = queueErnOf("invoice-import-dead");
        String liveErn = queueErnOf("invoice-import-live");
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString())).thenReturn(
                ListQueueResponse.builder().queues(List.of(
                        queue("invoice-import-dead", deadErn, Instant.now().minusSeconds(3600)),
                        queue("invoice-import-live", liveErn, Instant.now()))).total(2).build());
        when(euclidEsm.listSubscriptions("bucket-ern")).thenReturn(
                de.jensvogt.euclid.dto.esm.ListSubscriptionsResponse.builder().subscriptions(List.of(
                        // The queue this one delivered into was swept just now.
                        esmSubscription("sub-dead", deadErn),
                        // Already gone before this run started - the case that accumulates.
                        esmSubscription("sub-vanished", queueErnOf("invoice-import-vanished")),
                        // Another instance is still using this one.
                        esmSubscription("sub-live", liveErn),
                        // Somebody else's queue on the same bucket, and none of our business.
                        esmSubscription("sub-foreign", queueErnOf("another-app-queue"))))
                        .total(4).build());
        stubReceive(message(BUCKET_EVENT_BODY));
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false);

        container.start();

        verify(euclidEsm, timeout(1000)).unsubscribe("sub-dead");
        verify(euclidEsm, timeout(1000)).unsubscribe("sub-vanished");
        verify(euclidEsm, never()).unsubscribe("sub-live");
        verify(euclidEsm, never()).unsubscribe("sub-foreign");
    }

    @Test
    void topicListenerReceivesFromTheQueueAlreadySubscribedToTheTopic() throws Exception {
        stubReceive(message("published-body"));
        StringHandler handler = new StringHandler();
        container.registerTopic(handler, StringHandler.class.getMethod("handle", String.class),
                "order-events", "orders-app", 10, 0, true, 1);

        container.start();

        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertEquals("published-body", handler.received));
        verify(euclidEns, timeout(1000)).getTopicErn("order-events");
        verify(euclidEqs, timeout(1000)).getQueueErn("orders-app");
        verify(euclidEns, never()).subscribe(anyString(), anyString());
        verify(euclidEqs, never()).createQueue(anyString());
        verify(euclidEqs, timeout(1000)).deleteMessage("receipt-1");
    }

    @Test
    void topicListenerCreatesTheDeliveryQueueAndSubscribesIt() throws Exception {
        when(euclidEqs.getQueueErn("orders-app"))
                .thenThrow(new EuclidServiceException("eqs", "get-queue-ern", 404, "not found"));
        when(euclidEqs.createQueue("orders-app"))
                .thenReturn(CreateQueueResponse.builder().name("orders-app").ern("orders-app-ern").build());
        when(euclidEns.listSubscriptions("topic-ern"))
                .thenReturn(ListSubscriptionsResponse.builder().subscriptions(Collections.emptyList()).total(0).build());
        stubReceive(message("published-body"));
        StringHandler handler = new StringHandler();
        container.registerTopic(handler, StringHandler.class.getMethod("handle", String.class),
                "order-events", "orders-app", 10, 0, true, 1);

        container.start();

        verify(euclidEqs, timeout(1000)).createQueue("orders-app");
        verify(euclidEns, timeout(1000)).subscribe("topic-ern", "orders-app-ern");
        verify(euclidEqs, timeout(1000).atLeastOnce()).receiveMessages("orders-app-ern", 10, 0);
    }

    @Test
    void topicListenerIsNotStartedWhenTheTopicCannotBeResolved() throws Exception {
        when(euclidEns.getTopicErn("missing"))
                .thenThrow(new EuclidServiceException("ens", "get-topic-ern", 404, "not found"));
        stubReceive(message("published-body"));
        StringHandler handler = new StringHandler();
        container.registerTopic(handler, StringHandler.class.getMethod("handle", String.class),
                "missing", "orders-app", 10, 0, true, 1);

        container.start();

        await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertNull(handler.received));
        verify(euclidEqs, never()).receiveMessages(anyString(), anyLong(), anyLong());
    }

    @Test
    void topicListenerPassesTheFullEnvelopeAndKeepsTheMessageWhenAutoDeleteIsOff() throws Exception {
        Message theMessage = message("published-body");
        stubReceive(theMessage);
        MessageHandler handler = new MessageHandler();
        container.registerTopic(handler, MessageHandler.class.getMethod("handle", Message.class),
                "order-events", "orders-app", 10, 0, false, 1);

        container.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertEquals(theMessage, handler.received));
        verify(euclidEqs, never()).deleteMessage(anyString());
    }

    /**
     * The container asks for its clients through providers now, so a test hands it mocks the same
     * way the context does.
     */
    private static <T> ObjectProvider<T> providerOf(T instance) {
        @SuppressWarnings("unchecked")
        ObjectProvider<T> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(instance);
        when(provider.getIfAvailable()).thenReturn(instance);
        return provider;
    }

    private void registerBucket(Object bean, java.lang.reflect.Method method, String bucket, String prefix,
                                boolean directories) {
        container.registerBucket(bean, method, "invoice-import", bucket, prefix, directories, OBJECT_EVENTS,
                10, 0, 300, true, 1);
    }

    private void stubReceive(Message message) throws Exception {
        when(euclidEqs.receiveMessages(anyString(), anyLong(), anyLong()))
                .thenReturn(ReceiveMessagesResponse.builder().messages(List.of(message)).total(1).build())
                .thenReturn(ReceiveMessagesResponse.builder().messages(Collections.emptyList()).total(0).build());
    }

    private Message message(String body) {
        return new Message("ern", "queue-ern", "123", "AVAILABLE", "HIGH", body, "md5-body", "receipt-1",
                2, body.length(), "application/json", Map.of(), "md5-attributes", "now", "now", "now");
    }

    /** A queue ERN as the server forms it: the name is its last segment. */
    private static String queueErnOf(String queueName) {
        return "ern:eqs:eu-central-1:000000000000::queue:" + queueName;
    }

    /** A bucket subscription as list-subscriptions returns it. */
    private static de.jensvogt.euclid.dto.esm.model.Subscription esmSubscription(String ern, String targetErn) {
        return new de.jensvogt.euclid.dto.esm.model.Subscription(ern, "bucket-ern", "SQS", targetErn,
                Instant.now().toString(), Instant.now().toString());
    }

    /**
     * A queue as list-queues returns it, carrying the heartbeat tag a sweep judges it by.
     */
    private Queue queue(String name, String ern, Instant heartbeat) {
        return new Queue(name, "owner", ern, Map.of("euclid.listener.heartbeat", heartbeat.toString()), 0, 0, 0, 0, 0,
                30, 1024, 3, "", "MIDDLE", Instant.now().toString(), Instant.now().toString());
    }

    private record TestPayload(String name, int value) {
    }

    private record ObjectSummary(String key, long size) {
    }

    private static class TypedHandler {
        volatile TestPayload received;

        public void handle(TestPayload payload) {
            this.received = payload;
        }
    }

    private static class StringHandler {
        volatile String received;

        public void handle(String body) {
            this.received = body;
        }
    }

    private static class MessageHandler {
        volatile Message received;

        public void handle(Message message) {
            this.received = message;
        }
    }

    private static class EventHandler {
        volatile Event received;

        public void handle(Event event) {
            this.received = event;
        }
    }

    private static class ObjectHandler {
        volatile ObjectSummary received;

        public void handle(ObjectSummary object) {
            this.received = object;
        }
    }

    private static class FailingHandler {

        public void handle(Event event) {
            throw new IllegalStateException("handler failed for " + event.eventId());
        }
    }
}
