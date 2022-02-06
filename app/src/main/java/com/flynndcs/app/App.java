package com.flynndcs.app;

import com.flynndcs.app.contexts.TenantHandler;
import com.flynndcs.app.eventstore.CommandPersisterGroup;
import com.flynndcs.app.state.EventListenerGroup;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/**
 * Entry point class.
 *
 * @author Daniel Flynn (dflynn)
 */
public class App {
  private static final String SERVER_START_MESSAGE = "query server started at ";
  private static String metricsString;
  private static String HOSTNAME = "localhost";
  private static String DB_HOSTNAME = "localhost";
  private static final int PORT = 8088;
  private static ChronicleMap<Long, Long> countsMap;

  static {
    try {
      countsMap =
          ChronicleMapBuilder.of(Long.class, Long.class)
              .name("value-map")
              .entries(1000000L)
              .createPersistedTo(new File("./values.dat"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 0 && args[0] != null) {
      DB_HOSTNAME = args[0];
    }
    if (args.length > 1 && args[1] != null) {
      HOSTNAME = args[1];
    }
    PrometheusMeterRegistry metrics = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    SingleChronicleQueue queue = binary("queue").build();

    EventListenerGroup listenerGroup = new EventListenerGroup(DB_HOSTNAME, countsMap, 8, metrics);
    listenerGroup.start();

    CommandPersisterGroup persisterGroup = new CommandPersisterGroup(DB_HOSTNAME, 32, queue);
    persisterGroup.start();

    startServer(getServer(), countsMap, queue, metrics);
  }

  private static void startServer(
      Server server,
      ChronicleMap<Long, Long> countsMap,
      SingleChronicleQueue queue,
      PrometheusMeterRegistry metrics)
      throws Exception {
    ContextHandler metricsContext = new ContextHandler("/metrics");
    metricsContext.setContextPath("/metrics");
    metricsContext.setAllowNullPathInfo(true);
    metricsContext.setHandler(
        new AbstractHandler() {
          @Override
          public void handle(
              String s,
              Request baseRequest,
              HttpServletRequest request,
              HttpServletResponse response)
              throws IOException {
            baseRequest.setHandled(true);
            metricsString = metrics.scrape();
            response.setStatus(200);
            response.setContentLength(metricsString.getBytes(StandardCharsets.UTF_8).length);
            response.getOutputStream().write(metricsString.getBytes(StandardCharsets.UTF_8));
          }
        });

    ContextHandler context = new ContextHandler("/tenant");
    context.setContextPath("/tenant");
    context.setAllowNullPathInfo(true);

    context.setHandler(new TenantHandler(countsMap, metrics, queue));

    new JvmGcMetrics().bindTo(metrics);
    new JvmMemoryMetrics().bindTo(metrics);

    ContextHandlerCollection contexts = new ContextHandlerCollection(metricsContext, context);
    server.setHandler(contexts);
    server.start();
    System.out.println(SERVER_START_MESSAGE + HOSTNAME + ":" + PORT);
    server.join();
  }

  private static Server getServer() {
    QueuedThreadPool pool = new QueuedThreadPool(128);
    pool.setDetailedDump(true);
    Server server = new Server(pool);
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(PORT);
    connector.setHost(HOSTNAME);
    connector.setAcceptQueueSize(1024);
    server.addConnector(connector);
    return server;
  }
}
