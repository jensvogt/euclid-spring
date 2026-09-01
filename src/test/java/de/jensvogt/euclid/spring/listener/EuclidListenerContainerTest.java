package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.dto.ees.ReceiveEventsResponse;
import de.jensvogt.euclid.dto.ees.model.Event;
import de.jensvogt.euclid.dto.eqs.GetQueueErnResponse;
import de.jensvogt.euclid.dto.eqs.ReceiveMessagesResponse;
import de.jensvogt.euclid.dto.eqs.model.Message;
import de.jensvogt.euclid.exception.EuclidServiceException;
import de.jensvogt.euclid.module.ees.EuclidEes;
import de.jensvogt.euclid.module.eqs.EuclidEqs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EuclidListenerContainerTest {

    private static final List<String> OBJECT_EVENTS =
            List.of("esm.object.created", "esm.object.updated", "esm.object.deleted");

    private EuclidEqs euclidEqs;
    private EuclidEes euclidEes;
    private EuclidListenerContainer container;

    @BeforeEach
    void setUp() throws Exception {
        euclidEqs = mock(EuclidEqs.class);
        when(euclidEqs.getQueueErn(anyString())).thenReturn(GetQueueErnResponse.builder().ern("test-ern").build());

        euclidEes = mock(EuclidEes.class);

        @SuppressWarnings("unchecked")
        ObjectProvider<JsonMapper> objectMapperProvider = mock(ObjectProvider.class);
        when(objectMapperProvider.getIfAvailable(any())).thenReturn(new JsonMapper());

        container = new EuclidListenerContainer(euclidEqs, euclidEes, objectMapperProvider);
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
                "test-queue", 10, 0, true);

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
                "test-queue", 10, 0, true);

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
                "test-queue", 10, 0, true);

        container.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertEquals(theMessage, handler.received));
    }

    @Test
    void malformedPayloadIsNotDeletedAndHandlerNotInvoked() throws Exception {
        stubReceive(message("not-json"));
        TypedHandler handler = new TypedHandler();
        container.register(handler, TypedHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true);

        container.start();

        await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertFalse(handler.received != null));
        verify(euclidEqs, never()).deleteMessage(anyString());
    }

    @Test
    void passesFullEnvelopeForEventParameterAndAcknowledgesIt() throws Exception {
        Event theEvent = event();
        stubReceiveEvents(theEvent);
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false, true);

        container.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertEquals(theEvent, handler.received));
        verify(euclidEes, timeout(1000)).ackEvent("invoice-import", "evt-1");
    }

    @Test
    void convertsPayloadToTypedParameterIgnoringTheFieldsItDoesNotName() throws Exception {
        stubReceiveEvents(event());
        ObjectHandler handler = new ObjectHandler();
        registerBucket(handler, ObjectHandler.class.getMethod("handle", ObjectSummary.class), "invoices", "", false,
                true);

        container.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(
                () -> assertEquals(new ObjectSummary("invoices", "reports/q3.csv", 4211), handler.received));
    }

    @Test
    void subscribesToTheBucketFilteringPrefixAndDirectoryMarkers() throws Exception {
        stubReceiveEvents(event());
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "reports/", false,
                true);

        container.start();

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("bucketName", "invoices");
        expected.put("prefix", "reports/");
        expected.put("directory", false);
        verify(euclidEes, timeout(1000)).subscribeEvents("invoice-import", OBJECT_EVENTS, expected);
        verify(euclidEes, timeout(1000).atLeastOnce()).receiveEvents("invoice-import", 10, 0, 300);
    }

    @Test
    void keepsDirectoryMarkersOutOfTheFilterWhenTheyAreWanted() throws Exception {
        stubReceiveEvents(event());
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", true, true);

        container.start();

        verify(euclidEes, timeout(1000)).subscribeEvents("invoice-import", OBJECT_EVENTS,
                Map.of("bucketName", "invoices"));
    }

    @Test
    void failingHandlerLeavesTheEventUnacknowledged() throws Exception {
        stubReceiveEvents(event());
        FailingHandler handler = new FailingHandler();
        registerBucket(handler, FailingHandler.class.getMethod("handle", Event.class), "invoices", "", false, true);

        container.start();

        verify(euclidEes, timeout(1000).atLeastOnce()).receiveEvents(anyString(), anyLong(), anyLong(), anyLong());
        verify(euclidEes, never()).ackEvent(anyString(), anyString());
    }

    @Test
    void bucketListenerIsNotStartedWhenSubscribingFails() throws Exception {
        when(euclidEes.subscribeEvents(anyString(), anyList(), anyMap()))
                .thenThrow(new EuclidServiceException("ees", "subscribe-events", 403, "denied"));
        stubReceiveEvents(event());
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false, true);

        container.start();

        await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertNull(handler.received));
        verify(euclidEes, never()).receiveEvents(anyString(), anyLong(), anyLong(), anyLong());
    }

    private void registerBucket(Object bean, java.lang.reflect.Method method, String bucket, String prefix,
                                boolean directories, boolean autoAck) {
        container.registerBucket(bean, method, "invoice-import", bucket, prefix, directories, OBJECT_EVENTS,
                10, 0, 300, autoAck);
    }

    private void stubReceive(Message message) throws Exception {
        when(euclidEqs.receiveMessages(anyString(), anyLong(), anyLong()))
                .thenReturn(ReceiveMessagesResponse.builder().messages(List.of(message)).total(1).build())
                .thenReturn(ReceiveMessagesResponse.builder().messages(Collections.emptyList()).total(0).build());
    }

    private void stubReceiveEvents(Event event) throws Exception {
        when(euclidEes.receiveEvents(anyString(), anyLong(), anyLong(), anyLong()))
                .thenReturn(ReceiveEventsResponse.builder().events(List.of(event)).total(1).build())
                .thenReturn(ReceiveEventsResponse.builder().events(Collections.emptyList()).total(0).build());
    }

    private Message message(String body) {
        return new Message("ern", "queue-ern", "123", "AVAILABLE", "HIGH", body, "md5-body", "receipt-1",
                body.length(), "application/json", Map.of(), "md5-attributes", "now", "now", "now");
    }

    /**
     * An {@code esm.object.created} the way ESM publishes it, flat and carrying every field, so a
     * handler naming three of them exercises what a real payload does to the conversion.
     */
    private Event event() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("ern", "ern:esm:eu-central-1:000000000000:development:object:invoices/reports/q3.csv");
        payload.put("bucketErn", "ern:esm:eu-central-1:000000000000:development:bucket:invoices");
        payload.put("bucketName", "invoices");
        payload.put("key", "reports/q3.csv");
        payload.put("prefix", "reports/");
        payload.put("directory", false);
        payload.put("size", 4211);
        payload.put("contentType", "text/csv");
        payload.put("md5Sum", "2aacead6864fa88adab90b825464f87c");
        payload.put("owner", "admin");
        payload.put("userId", "admin");
        payload.put("accountId", "000000000000");
        payload.put("region", "eu-central-1");
        payload.put("namespace", "development");
        payload.put("eventTime", "2026-09-01T14:57:20.842631687Z");
        return new Event("evt-1", "esm.object.created", "esm", payload, 0, "2026-09-01T14:57:20Z");
    }

    private record TestPayload(String name, int value) {
    }

    private record ObjectSummary(String bucketName, String key, long size) {
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
