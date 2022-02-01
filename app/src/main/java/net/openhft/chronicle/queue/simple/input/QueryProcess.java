package net.openhft.chronicle.queue.simple.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.queue.simple.input.domain.Command;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;
import net.openhft.chronicle.queue.simple.input.http.QueryHandler;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Process implementing read model for queries to items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class QueryProcess {
  private static final PrometheusMeterRegistry metrics =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
  private static final String QUEUE = "queue";
  private static final String UPSERT = "upsert";
  private static final String ITEM_PATH = "/item";
  private static final String HEALTHCHECK_PATH = "/healthcheck";
  private static final String REPLAY_TIME_MESSAGE = " micros elapsed while replaying commands";
  private static final String REPLAY_COUNT_MESSAGE = " commands replayed";
  private static final String AGGREGATES_PRESENT = " aggregates present";
  private static final String SERVER_START_MESSAGE = "query server started at ";

  private static String HOSTNAME = "localhost";
  private static String DB_HOSTNAME = "localhost";
  private static final String pgUser = "postgres";
  private static final String pgPass = "postgres";
  private static final int PORT = 8089;

  private static final LongValue longValue = Values.newHeapInstance(LongValue.class);

  private static QueryHandler QUERY_HANDLER;

  private static final Command COMMAND = new Command();
  private static CommandDTO COMMAND_DTO = new CommandDTO();

  private static final ObjectReader READER = new ObjectMapper().readerFor(CommandDTO.class);

  private static boolean HEALTHY = false;

  public static void main(String[] args) throws IOException, SQLException {
    replayCommandsFromEventStore(args);
    HEALTHY = true;
    startServer(getServer());
  }

  private static Command parseDTOToCommand() {
    COMMAND.setAction(QueryProcess.COMMAND_DTO.getAction());
    COMMAND.setId(QueryProcess.COMMAND_DTO.getId());
    if (QueryProcess.COMMAND_DTO.getValue() != null) {
      COMMAND.setValue(QueryProcess.COMMAND_DTO.getValue());
    }
    return COMMAND;
  }

  private static void replayCommandsFromEventStore(String[] args) throws SQLException, IOException {
    if (args.length > 0 && args[0] != null) {
      DB_HOSTNAME = args[0];
    }
    if (args.length > 1 && args[1] != null) {
      HOSTNAME = args[1];
    }
    String pgUrl = "jdbc:postgresql://" + DB_HOSTNAME + ":5432/postgres";
    Connection connection = DriverManager.getConnection(pgUrl, pgUser, pgPass);
    long NANOS_START = System.nanoTime();
    int commandsReplayed = 0;

    // get map for events to stream into
    ChronicleMap<LongValue, CharSequence> valuesMap =
        ChronicleMapBuilder.of(LongValue.class, CharSequence.class)
            .name("value-map")
            .entries(1000000L)
            .averageValue("value")
            .createPersistedTo(new File("./map.dat"));
    QUERY_HANDLER = new QueryHandler(valuesMap);

    // replay commands from event store
    PreparedStatement statement =
        connection.prepareStatement("SELECT * FROM event ORDER BY written_at DESC");
    ResultSet rs = statement.executeQuery();
    int eventColumn = rs.findColumn("event");
    while (rs.next()) {
      String eventJson = rs.getString(eventColumn);
      try {
        COMMAND_DTO = READER.readValue(eventJson);
        handleCommand(valuesMap, parseDTOToCommand());
        commandsReplayed++;
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
    }
    System.out.println((System.nanoTime() - NANOS_START) / 1000 + REPLAY_TIME_MESSAGE);
    System.out.println(commandsReplayed + REPLAY_COUNT_MESSAGE);
    System.out.println(valuesMap.size() + AGGREGATES_PRESENT);
  }

  private static void startServer(HttpServer server) {
    server.createContext(ITEM_PATH, QUERY_HANDLER);
    server.createContext(
        HEALTHCHECK_PATH,
        httpExchange -> {
          if (HEALTHY) {
            httpExchange.sendResponseHeaders(200, "healthy".length());
          } else {
            httpExchange.sendResponseHeaders(503, "unhealthy".length());
          }
        });
    server.setExecutor(newSingleThreadExecutor());
    server.start();
    System.out.println(SERVER_START_MESSAGE + HOSTNAME + ":" + PORT);
  }

  private static HttpServer getServer() throws IOException {
    return HttpServer.create(new InetSocketAddress(HOSTNAME, PORT), 0);
  }

  private static void handleCommand(
      ChronicleMap<LongValue, CharSequence> valuesMap, Command parsedCommand) {
    // business logic here, placed into in-memory data structure
    // command -> data to be read
    if (UPSERT.equalsIgnoreCase(parsedCommand.getAction())) {
      longValue.setValue(parsedCommand.getId());
      valuesMap.put(longValue, parsedCommand.getValue());
    } else {
      valuesMap.remove(longValue);
    }
  }
}
