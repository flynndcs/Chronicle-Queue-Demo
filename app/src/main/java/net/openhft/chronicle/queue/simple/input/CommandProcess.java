package net.openhft.chronicle.queue.simple.input;

import com.sun.net.httpserver.HttpServer;
import net.openhft.chronicle.queue.simple.input.http.CommandHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/**
 * Command path for items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class CommandProcess {
  private static final String QUEUE = "queue";
  private static final String COMMAND_PATH = "/item";
  private static final String SERVER_START_MESSAGE = "Server started at localhost:";
  private static String DB_HOSTNAME = "localhost";
  private static String HOSTNAME = "localhost";
  private static CommandHandler COMMAND_HANDLER;
  private static final int PORT = 8088;

  private static final String pgUser = "postgres";
  private static final String pgPass = "postgres";

  public static void main(String[] args) throws IOException, SQLException {
    startServer(createServer(args));
  }

  private static HttpServer createServer(String[] args) throws IOException, SQLException {
    if (args.length > 0 && args[0] != null) {
      DB_HOSTNAME = args[0];
    }
    if (args.length > 1 && args[1] != null) {
      HOSTNAME = args[1];
    }
    System.out.println("DB Hostname: " + DB_HOSTNAME);
    System.out.println("App Hostname: " + HOSTNAME);
    String pgUrl = "jdbc:postgresql://" + DB_HOSTNAME + ":5432/postgres";
    Connection connection = DriverManager.getConnection(pgUrl, pgUser, pgPass);
    COMMAND_HANDLER = new CommandHandler(binary(QUEUE).build().acquireAppender(), connection);
    return HttpServer.create(new InetSocketAddress(HOSTNAME, PORT), 0);
  }

  private static void startServer(HttpServer server) {
    server.createContext(COMMAND_PATH, COMMAND_HANDLER);
    server.setExecutor(newSingleThreadExecutor());
    server.start();
    System.out.println(SERVER_START_MESSAGE + PORT);
  }
}
