package com.flynndcs.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.flynndcs.app.contexts.ItemHandler;
import com.flynndcs.app.dto.CommandDTO;
import com.flynndcs.app.eventstore.ItemEventListener;
import com.flynndcs.app.state.ChronicleMapPersister;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.queue.simple.input.CommandPersister;
import net.openhft.chronicle.values.Values;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Entry point class.
 *
 * @author Daniel Flynn (dflynn)
 */
public class App {
  private static final PrometheusMeterRegistry metrics =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
  private static final String REPLAY_TIME_MESSAGE = " micros elapsed while replaying commands";
  private static final String REPLAY_COUNT_MESSAGE = " commands replayed";
  private static final String AGGREGATES_PRESENT = " aggregates present";
  private static final String SERVER_START_MESSAGE = "query server started at ";
  private static String metricsString;
  private static final LongValue longValue = Values.newHeapInstance(LongValue.class);
  private static ChronicleMap<LongValue, CharSequence> valuesMap;
  private static ChronicleMap<ByteBuffer, Boolean> eventsMap;
  private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
  private static CommandPersister PERSISTER;

  static {
    try {
      valuesMap =
          ChronicleMapBuilder.of(LongValue.class, CharSequence.class)
              .name("value-map")
              .entries(1000000L)
              .averageValue("value")
              .createPersistedTo(new File("./values.dat"));
      eventsMap =
          ChronicleMapBuilder.of(ByteBuffer.class, boolean.class)
              .name("value-map")
              .entries(1000000L)
              .averageValue(true)
              .createPersistedTo(new File("./events.dat"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static final ItemHandler ITEM_HANDLER = new ItemHandler(valuesMap, metrics);

  private static ItemEventListener EVENT_LISTENER;
  private static String HOSTNAME = "localhost";
  private static String DB_HOSTNAME = "localhost";
  private static final String pgUser = "postgres";
  private static final String pgPass = "postgres";
  private static final int PORT = 8088;

  private static final ThreadLocal<CommandDTO> COMMAND_DTO =
      ThreadLocal.withInitial(() -> Values.newHeapInstance(CommandDTO.class));

  private static final ObjectReader READER = new ObjectMapper().readerFor(CommandDTO.class);

  public static void main(String[] args) throws Exception {
    if (args.length > 0 && args[0] != null) {
      DB_HOSTNAME = args[0];
    }
    if (args.length > 1 && args[1] != null) {
      HOSTNAME = args[1];
    }
    PERSISTER = new CommandPersister(DB_HOSTNAME);
    EVENT_LISTENER = new ItemEventListener(DB_HOSTNAME, valuesMap, eventsMap, metrics);
    replayCommandsFromEventStore();
    startServer(getServer());
  }

  private static void replayCommandsFromEventStore() throws SQLException {
    Connection connection =
        DriverManager.getConnection(
            "jdbc:postgresql://" + DB_HOSTNAME + ":5432/postgres", pgUser, pgPass);
    long NANOS_START = System.nanoTime();
    int commandsReplayed = 0;
    System.out.println("Connected to Postgres at: " + DB_HOSTNAME + ":5432");

    ResultSet rs =
        connection.prepareStatement("SELECT * FROM event ORDER BY written_at DESC").executeQuery();
    while (rs.next()) {
      try {
        COMMAND_DTO.set(READER.readValue(rs.getString(rs.findColumn("event"))));
        longValue.setValue(COMMAND_DTO.get().getId());
        ChronicleMapPersister.persistToValuesMap(
            longValue, READER.readValue(rs.getString(rs.findColumn("event"))), valuesMap);
        commandsReplayed++;
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
    }
    System.out.println((System.nanoTime() - NANOS_START) / 1000 + REPLAY_TIME_MESSAGE);
    System.out.println(commandsReplayed + REPLAY_COUNT_MESSAGE);
    System.out.println(valuesMap.size() + AGGREGATES_PRESENT);
  }

  private static void startServer(Server server) throws Exception {
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

    ContextHandler context = new ContextHandler("/item");
    context.setContextPath("/item");
    context.setAllowNullPathInfo(true);
    context.setHandler(ITEM_HANDLER);

    new JvmGcMetrics().bindTo(metrics);
    new JvmMemoryMetrics().bindTo(metrics);

    ContextHandlerCollection contexts = new ContextHandlerCollection(metricsContext, context);
    server.setHandler(contexts);

    executor.scheduleAtFixedRate(PERSISTER, 20, 5, TimeUnit.SECONDS);
//    executor.scheduleAtFixedRate(EVENT_LISTENER, 20, 5, TimeUnit.SECONDS);

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
