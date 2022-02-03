package net.openhft.chronicle.queue.simple.input;

import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.queue.simple.input.http.CommandHandler;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Command path for items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class CommandProcess {
  private static final PrometheusMeterRegistry metrics =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
  private static final String SERVER_START_MESSAGE = "Server started at localhost:";
  private static String DB_HOSTNAME = "localhost";
  private static String HOSTNAME = "localhost";
  private static final int PORT = 8088;
  private static final File file = new File("./map.dat");
  private static ChronicleMap<LongValue, CharSequence> map;
  private static Connection connection;

  private static final String pgUser = "postgres";
  private static final String pgPass = "postgres";
  private static String pgUrl = "jdbc:postgresql://" + DB_HOSTNAME + ":5432/postgres";

  private static String metricsString;

  static {
    try {
      map =
          ChronicleMapBuilder.of(LongValue.class, CharSequence.class)
              .name("value-map")
              .entries(1000000L)
              .averageValue("value")
              .createPersistedTo(file);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    startServer(createServer(args));
  }

  private static Server createServer(String[] args) throws SQLException {
    if (args.length > 0 && args[0] != null) {
      DB_HOSTNAME = args[0];
    }
    if (args.length > 1 && args[1] != null) {
      HOSTNAME = args[1];
    }
    System.out.println("DB Hostname: " + DB_HOSTNAME);
    System.out.println("App Hostname: " + HOSTNAME);
    pgUrl = "jdbc:postgresql://" + DB_HOSTNAME + ":5432/postgres";
    connection = DriverManager.getConnection(pgUrl, pgUser, pgPass);

    QueuedThreadPool pool = new QueuedThreadPool(64);
    pool.setDetailedDump(true);
    Server server = new Server(pool);
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(PORT);
    connector.setHost(HOSTNAME);
    connector.setAcceptQueueSize(1024);
    server.addConnector(connector);
    return server;
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
    context.setHandler(new CommandHandler(map, metrics));

    new JvmGcMetrics().bindTo(metrics);
    new JvmMemoryMetrics().bindTo(metrics);

    ContextHandlerCollection contexts = new ContextHandlerCollection(metricsContext, context);
    server.setHandler(contexts);
    server.start();
    System.out.println(SERVER_START_MESSAGE + PORT);
    server.join();
  }
}
