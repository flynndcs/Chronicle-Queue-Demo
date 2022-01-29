package net.openhft.chronicle.queue.simple.input;

import com.sun.net.httpserver.HttpServer;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.simple.input.http.CommandHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Command path for items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class CommandProcess {
  public static void main(String[] args) {
    //get queue for writing
    String path = "temp";
    SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
    ExcerptAppender appender = queue.acquireAppender();

    //set up http server to ingest commands
    HttpServer server;
    int port = 8088;
    try {
      server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    ExecutorService executor = Executors.newSingleThreadExecutor();
    server.createContext("/item", new CommandHandler(appender));
    server.setExecutor(executor);
    server.start();
    System.out.println("server started at localhost:" + port);
  }
}
