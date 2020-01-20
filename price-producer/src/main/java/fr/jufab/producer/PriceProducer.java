package fr.jufab.producer;

import io.opentracing.Tracer;
import io.opentracing.contrib.jms2.TracingJMSProducer;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.jms.*;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author jufab
 * @version 1.0
 */
public class PriceProducer implements Runnable {
    @Inject
    ConnectionFactory connectionFactory;
    @Inject
    Tracer tracer;

    Logger logger = Logger.getLogger(PriceProducer.class.getName());

    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    void onStart(@Observes StartupEvent ev) {
        scheduler.scheduleWithFixedDelay(this, 0L, 20L, TimeUnit.SECONDS);
    }

    void onStop(@Observes ShutdownEvent ev) {
        scheduler.shutdown();
    }

    @Override
    public void run() {
        try (JMSContext context = connectionFactory.createContext(Session.AUTO_ACKNOWLEDGE)) {
            JMSProducer jmsProducer = context.createProducer();
            TracingJMSProducer producer = new TracingJMSProducer(jmsProducer, context, tracer);
            producer.send(context.createQueue("prices"), Integer.toString(random.nextInt(100)));
        }
    }
}
