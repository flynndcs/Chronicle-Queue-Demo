package net.openhft.chronicle.queue.simple.input;

import com.sun.net.httpserver.HttpServer;
import net.openhft.chronicle.queue.simple.input.http.CommandHandler;

import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/**
 * Command path for items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class CommandProcess {
  private static final String HOSTNAME = "localhost";
  private static final String QUEUE = "queue";
  private static final String COMMAND_PATH = "/item";
  private static final String SERVER_START_MESSAGE = "Server started at localhost:";
  private static final CommandHandler COMMAND_HANDLER = new CommandHandler(binary(QUEUE).build().acquireAppender());
  private static final int PORT = 8088;

  public static void main(String[] args) throws IOException {
    startServer(createServer());
  }

  private static HttpServer createServer() throws IOException {
    return HttpServer.create(new InetSocketAddress(HOSTNAME, PORT), 0);
  }

  private static void startServer(HttpServer server) {
    server.createContext(COMMAND_PATH, COMMAND_HANDLER);
    server.setExecutor(newSingleThreadExecutor());
    server.start();
    System.out.println(SERVER_START_MESSAGE + PORT);
  }
}
