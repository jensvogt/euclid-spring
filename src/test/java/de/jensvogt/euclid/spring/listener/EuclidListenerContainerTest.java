package de.jensvogt.euclid.spring.listener;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.jensvogt.euclid.dto.com.Variant;
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
import de.jensvogt.euclid.module.eap.EuclidEap;
import de.jensvogt.euclid.module.emo.EuclidEmo;
import de.jensvogt.euclid.module.esm.EuclidEsm;
import de.jensvogt.euclid.module.ens.EuclidEns;
import de.jensvogt.euclid.module.eqs.EuclidEqs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
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
    private EuclidEmo euclidEmo;

    /**
     * The autoscaler's own copy of the load figures goes here rather than through EMO, which
     * aggregates into five-minute buckets before anything can read them.
     */
    private EuclidEap euclidEap;
    private EuclidListenerContainer container;

    private Logger containerLogger;
    private ListAppender<ILoggingEvent> logAppender;
    private Level loggerLevelBefore;

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
        when(euclidEqs.createQueue(anyString(), anyLong(), anyLong(), anyLong(), anyString(), anyLong(), anyString(), anyBoolean()))
                .thenReturn(CreateQueueResponse.builder().name("delivery").ern("delivery-ern").build());
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString(), anyString(), anyBoolean()))
                .thenReturn(ListQueueResponse.builder().queues(Collections.emptyList()).total(0).build());

        euclidEns = mock(EuclidEns.class);
        when(euclidEns.getTopicErn(anyString())).thenReturn(GetTopicErnResponse.builder().ern("topic-ern").build());
        when(euclidEns.listSubscriptions(anyString())).thenReturn(ListSubscriptionsResponse.builder()
                .subscriptions(List.of(new Subscription("sub-ern", "topic-ern", "SQS", "test-ern", "now", "now")))
                .total(1).build());

        @SuppressWarnings("unchecked")
        ObjectProvider<JsonMapper> objectMapperProvider = mock(ObjectProvider.class);
        when(objectMapperProvider.getIfAvailable(any())).thenReturn(new JsonMapper());

        euclidEap = mock(EuclidEap.class);

        container = new EuclidListenerContainer(providerOf(euclidEqs), providerOf(euclidEsm), providerOf(euclidEns), providerOf(euclidEmo),
                providerOf(euclidEap), objectMapperProvider);
    }

    @AfterEach
    void tearDown() {
        container.stop();
        releaseContainerLog();
    }

    /**
     * The count the autoscaler reads as "do not stop me". It has to come back down however the
     * handler leaves - a count that only ever rises pins the pool at its ceiling for the life of
     * the process, and nothing else would say so.
     */
    @Test
    void handlersInFlightAreCountedWhileTheyRunAndNotAfterwards() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        stubReceive(message("{\"name\":\"abc\",\"value\":5}"));
        BlockingHandler handler = new BlockingHandler(entered, release);
        container.register(handler, BlockingHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true, 1);

        assertEquals(0L, container.activeHandlers());
        container.start();

        assertTrue(entered.await(2, TimeUnit.SECONDS));
        assertEquals(1L, container.activeHandlers());

        release.countDown();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertEquals(0L, container.activeHandlers()));
    }

    /** The finally that decrements has to survive a handler that throws, or the count never falls. */
    @Test
    void aHandlerThatFailsStillStopsBeingCounted() throws Exception {
        stubReceive(message("{\"name\":\"abc\",\"value\":5}"));
        FailingPayloadHandler handler = new FailingPayloadHandler();
        container.register(handler, FailingPayloadHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(euclidEqs, atLeastOnce())
                .receiveMessages(anyString(), anyLong(), anyLong()));
        await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertEquals(0L, container.activeHandlers()));
    }

    /** A handler that waits to be let go, so the count can be read while one is in flight. */
    private static class BlockingHandler {

        private final CountDownLatch entered;
        private final CountDownLatch release;

        private BlockingHandler(CountDownLatch entered, CountDownLatch release) {
            this.entered = entered;
            this.release = release;
        }

        public void handle(TestPayload payload) {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Handler was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * A handler abandoned by stop() is the shutdown working, not the handler failing - and there is
     * one of these per message in flight, so reported as errors they are the loudest thing in the
     * log of a clean shutdown. What the container sees is what EuclidCalls leaves behind: the
     * interrupt flag re-set, and the InterruptedException wrapped in something of the caller's own.
     */
    @Test
    void aMessageAbandonedDuringShutdownIsNotReportedAsAFailure() throws Exception {
        ListAppender<ILoggingEvent> log = captureContainerLog();
        stubReceive(message("{\"name\":\"abc\",\"value\":5}"));
        InterruptingHandler handler = new InterruptingHandler();
        container.register(handler, InterruptingHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertTrue(loggedAt(log, Level.INFO, "abandoned message")));
        assertFalse(loggedAt(log, Level.ERROR, "failed to handle message"));
        // Not deleted, so the lease expires and somebody else gets it - which is what makes
        // abandoning it safe in the first place.
        verify(euclidEqs, never()).deleteMessage(anyString());
    }

    /** The other half of the branch above: a handler that fails on its own is still an error. */
    @Test
    void aHandlerThatFailsOnItsOwnIsStillReportedAsAFailure() throws Exception {
        ListAppender<ILoggingEvent> log = captureContainerLog();
        stubReceive(message("{\"name\":\"abc\",\"value\":5}"));
        FailingPayloadHandler handler = new FailingPayloadHandler();
        container.register(handler, FailingPayloadHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertTrue(loggedAt(log, Level.ERROR, "failed to handle message")));
        verify(euclidEqs, never()).deleteMessage(anyString());
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

    /**
     * Stopping a queue is an operator's instruction, not a fault. EQS refuses the receive with a
     * 409, which arrives here as an exception - and without recognising it, the listener would
     * treat a deliberately stopped queue as a failure and retry it as fast as the refusals came
     * back, which is harder than it polls a running one.
     */
    @Test
    void stoppedQueueIsNotPolledInATightLoop() throws Exception {
        when(euclidEqs.receiveMessages(anyString(), anyLong(), anyLong()))
                .thenThrow(new EuclidServiceException("eqs", "receive-messages", 409, "Queue is stopped, ern: test-ern"));
        TypedHandler handler = new TypedHandler();
        container.register(handler, TypedHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        // A second of a stopped queue at the 5s retry is one or two attempts. Unrecognised it
        // would be thousands, since the refusal comes back without the long poll being held.
        Thread.sleep(1000);
        verify(euclidEqs, atMost(2)).receiveMessages(anyString(), anyLong(), anyLong());
    }

    /** Nothing is polled to notice the queue came back - the next receive succeeding is what says so. */
    @Test
    void stoppedQueueResumesOnceItIsAvailableAgain() throws Exception {
        when(euclidEqs.receiveMessages(anyString(), anyLong(), anyLong()))
                .thenThrow(new EuclidServiceException("eqs", "receive-messages", 409, "Queue is stopped, ern: test-ern"))
                .thenReturn(ReceiveMessagesResponse.builder()
                        .messages(List.of(message("{\"name\":\"abc\",\"value\":5}"))).total(1).build());
        TypedHandler handler = new TypedHandler();
        container.register(handler, TypedHandler.class.getMethod("handle", TestPayload.class),
                "test-queue", 10, 0, true, 1);

        container.start();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertEquals(new TestPayload("abc", 5), handler.received));
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
        ArgumentCaptor<String> dlqName = ArgumentCaptor.forClass(String.class);
        verify(euclidEqs, timeout(1000)).createQueue(queueName.capture(), eq(300L), anyLong(), anyLong(),
                dlqName.capture(), anyLong(), eq("MEDIUM"), eq(true));
        // The run id keeps this run's queue apart from one another run of the same listener owns.
        assertTrue(queueName.getValue().startsWith("invoice-import-"), queueName.getValue());

        // The dead letter queue carries no run id, so it outlives the run whose failures it holds -
        // and without one at all EQS has nowhere to put a message that keeps failing, so it hands
        // it back for ever. A poison event was seen redelivered nineteen times against a limit of
        // three, for want of this argument.
        assertEquals("invoice-import-dlq", dlqName.getValue());
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
        assertEquals(1, event.attempts());
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
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString(), anyString(), anyBoolean())).thenReturn(
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

        // Delivery queues are created internal so they stay out of listings people read, which
        // also keeps them out of this one unless it asks. A sweep that does not ask sees nothing,
        // deletes nothing - and, worse, treats every subscription as orphaned because no queue
        // survived to keep it.
        verify(euclidEqs, timeout(1000)).listQueues(anyString(), anyLong(), anyLong(), anyString(), anyString(), eq(true));
    }

    /**
     * One queue that cannot be deleted must not stop the sweep.
     *
     * <p>The delete used to be inside the try that wraps the whole sweep, so a single failure -
     * the permission missing, somebody deleting it underneath us, a busy server - abandoned the
     * remaining queues and the subscription cleanup with them. Nothing was ever swept again and
     * the debris only grew: 62 abandoned queues holding 11,946 messages on one development
     * installation, the oldest three days old.
     */
    /**
     * The dead letter queue shares the listener's queue prefix and never carries a heartbeat, so a
     * sweep judging it by age would delete it within minutes - throwing away exactly the failures
     * it was created to keep, and doing so on the next start after the run that produced them.
     */
    @Test
    void theDeadLetterQueueIsNotSweptAwayWithTheRunThatFailed() throws Exception {
        String dlqErn = queueErnOf("invoice-import-dlq");
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString(), anyString(), anyBoolean())).thenReturn(
                ListQueueResponse.builder().queues(List.of(
                        // Old enough to look abandoned, and with no heartbeat - which is what a
                        // dead letter queue always looks like, since nothing beats for it.
                        queue("invoice-import-dlq", dlqErn, Instant.now().minusSeconds(3600)),
                        queue("invoice-import-dead", queueErnOf("invoice-import-dead"), Instant.now().minusSeconds(3600)))).total(2).build());
        stubReceive(message(BUCKET_EVENT_BODY));
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false);

        container.start();

        // The abandoned run's queue still goes...
        verify(euclidEqs, timeout(1000)).deleteQueue(queueErnOf("invoice-import-dead"));
        // ...and the dead letter queue stays.
        verify(euclidEqs, never()).deleteQueue(dlqErn);
    }

    @Test
    void oneQueueThatWillNotDeleteDoesNotStopTheSweep() throws Exception {
        String firstErn = queueErnOf("invoice-import-first");
        String secondErn = queueErnOf("invoice-import-second");
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString(), anyString(), anyBoolean())).thenReturn(
                ListQueueResponse.builder().queues(List.of(
                        queue("invoice-import-first", firstErn, Instant.now().minusSeconds(3600)),
                        queue("invoice-import-second", secondErn, Instant.now().minusSeconds(3600)))).total(2).build());
        doThrow(new IOException("eqs:delete-queue refused")).when(euclidEqs).deleteQueue(firstErn);
        stubReceive(message(BUCKET_EVENT_BODY));
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false);

        container.start();

        // The one after the failure is still swept, which is the whole point.
        verify(euclidEqs, timeout(1000)).deleteQueue(secondErn);
    }

    /**
     * A queue that could not be deleted keeps its subscription.
     *
     * <p>Removing the subscription of a queue that still exists is the worst of the three outcomes:
     * the queue is then fed by nothing and drained by nothing, so its messages sit there for ever
     * and still count toward the backlog the autoscaler reads - which is how a pool ends up
     * starting an instance every minute for work nobody will ever do.
     */
    @Test
    void aQueueThatCouldNotBeDeletedKeepsItsSubscription() throws Exception {
        String stubbornErn = queueErnOf("invoice-import-stubborn");
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString(), anyString(), anyBoolean())).thenReturn(
                ListQueueResponse.builder().queues(List.of(
                        queue("invoice-import-stubborn", stubbornErn, Instant.now().minusSeconds(3600)))).total(1).build());
        when(euclidEsm.listSubscriptions("bucket-ern")).thenReturn(
                de.jensvogt.euclid.dto.esm.ListSubscriptionsResponse.builder().subscriptions(List.of(
                        esmSubscription("sub-stubborn", stubbornErn))).total(1).build());
        doThrow(new IOException("eqs:delete-queue refused")).when(euclidEqs).deleteQueue(stubbornErn);
        stubReceive(message(BUCKET_EVENT_BODY));
        EventHandler handler = new EventHandler();
        registerBucket(handler, EventHandler.class.getMethod("handle", Event.class), "invoices", "", false);

        container.start();

        verify(euclidEqs, timeout(1000)).deleteQueue(stubbornErn);
        // Both survive, so the next run can try again. Leaving the queue without its subscription
        // is the state that cannot repair itself.
        verify(euclidEsm, never()).unsubscribe("sub-stubborn");
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
        when(euclidEqs.listQueues(anyString(), anyLong(), anyLong(), anyString(), anyString(), anyBoolean())).thenReturn(
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
        verify(euclidEqs, never()).createQueue(anyString(), anyLong(), anyLong(), anyLong(), anyString(), anyLong(),
                anyString(), anyBoolean());
        verify(euclidEqs, timeout(1000)).deleteMessage("receipt-1");
    }

    @Test
    void topicListenerCreatesTheDeliveryQueueAndSubscribesIt() throws Exception {
        when(euclidEqs.getQueueErn("orders-app"))
                .thenThrow(new EuclidServiceException("eqs", "get-queue-ern", 404, "not found"));
        when(euclidEqs.createQueue(eq("orders-app"), anyLong(), anyLong(), anyLong(), anyString(), anyLong(),
                anyString(), anyBoolean()))
                .thenReturn(CreateQueueResponse.builder().name("orders-app").ern("orders-app-ern").build());
        when(euclidEns.listSubscriptions("topic-ern"))
                .thenReturn(ListSubscriptionsResponse.builder().subscriptions(Collections.emptyList()).total(0).build());
        stubReceive(message("published-body"));
        StringHandler handler = new StringHandler();
        container.registerTopic(handler, StringHandler.class.getMethod("handle", String.class),
                "order-events", "orders-app", 10, 0, true, 1);

        container.start();

        // Internal: a topic listener's delivery queue is plumbing behind the subscription, and
        // listing it invites somebody to act on a queue that is not theirs to act on.
        verify(euclidEqs, timeout(1000)).createQueue(eq("orders-app"), anyLong(), anyLong(), anyLong(),
                eq("orders-app-dlq"), anyLong(), eq("MEDIUM"), eq(true));
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

    /**
     * Collects what the container logs, at DEBUG so that the level a message was logged at is
     * something the test can see rather than something the configuration has already decided.
     */
    private ListAppender<ILoggingEvent> captureContainerLog() {
        containerLogger = (Logger) LoggerFactory.getLogger(EuclidListenerContainer.class);
        loggerLevelBefore = containerLogger.getLevel();
        containerLogger.setLevel(Level.DEBUG);

        logAppender = new ListAppender<>();
        logAppender.start();
        containerLogger.addAppender(logAppender);
        return logAppender;
    }

    private void releaseContainerLog() {
        if (containerLogger == null) {
            return;
        }
        containerLogger.detachAppender(logAppender);
        containerLogger.setLevel(loggerLevelBefore);
        containerLogger = null;
    }

    private static boolean loggedAt(ListAppender<ILoggingEvent> log, Level level, String contains) {
        return log.list.stream()
                .anyMatch(event -> event.getLevel() == level && event.getFormattedMessage().contains(contains));
    }

    private void stubReceive(Message message) throws Exception {
        when(euclidEqs.receiveMessages(anyString(), anyLong(), anyLong()))
                .thenReturn(ReceiveMessagesResponse.builder().messages(List.of(message)).total(1).build())
                .thenReturn(ReceiveMessagesResponse.builder().messages(Collections.emptyList()).total(0).build());
    }

    private Message message(String body) {
        return new Message("ern", "queue-ern", "123", "AVAILABLE", "HIGH", body, "receipt-1",
                body.length(), 1, "application/json", Map.of(), Map.of(),"now", "now", "now");
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
                30, 1024, 3, "", "MEDIUM", Instant.now().toString(), Instant.now().toString());
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

    /** A handler caught by stop(), as it looks by the time the container sees it. */
    private static class InterruptingHandler {

        public void handle(TestPayload payload) {
            // The order EuclidCalls leaves things in: flag re-set, InterruptedException wrapped.
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while trying to list objects",
                    new InterruptedException());
        }
    }

    /** A handler that fails for a reason of its own, with nothing interrupted. */
    private static class FailingPayloadHandler {

        public void handle(TestPayload payload) {
            throw new IllegalStateException("Could not parse it");
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
