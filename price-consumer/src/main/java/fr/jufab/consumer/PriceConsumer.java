package fr.jufab.consumer;

import io.opentracing.Tracer;
import io.opentracing.contrib.jms.common.TracingMessageConsumer;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * @author jufab
 * @version 1.0
 */
@ApplicationScoped
public class PriceConsumer implements Runnable {
    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    Tracer tracer;

    Connection connection;

    Logger logger = Logger.getLogger(PriceConsumer.class.getName());

    private final ExecutorService scheduler = Executors.newSingleThreadExecutor();

    void onStart(@Observes StartupEvent ev) throws JMSException {
        connection = connectionFactory.createConnection();
        connection.start();
        scheduler.submit(this);
    }

    void onStop(@Observes ShutdownEvent ev) throws JMSException {
        connection.close();
        scheduler.shutdown();
    }

    @Override
    public void run() {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageConsumer messageConsumer = session.createConsumer(session.createQueue("prices"));
            TracingMessageConsumer consumer = new TracingMessageConsumer(messageConsumer, tracer);
            while (true) {
                Message message = consumer.receive();
                if (message == null) return;
                logger.info(message.getBody(String.class));
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}
