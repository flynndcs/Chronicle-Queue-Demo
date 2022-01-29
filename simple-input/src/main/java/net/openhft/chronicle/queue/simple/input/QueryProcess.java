package net.openhft.chronicle.queue.simple.input;

import com.sun.net.httpserver.HttpServer;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.simple.input.domain.ParsedCommand;
import net.openhft.chronicle.queue.simple.input.http.QueryHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Process implementing read model for queries to items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class QueryProcess {
  public static void main(String[] args) {
    // get queue from beginning
    String path = "temp";
    SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
    ExcerptTailer tailer = queue.createTailer();

    // create empty in memory data structures
    Map<Integer, String> itemsMap = new HashMap<>();

    // starting from first command, prepare to parse into values for aggregate
    String command = tailer.readText();
    ParsedCommand parsedCommand;
    Integer id;
    String value;
    String action;

    // replay commands from queue to assemble aggregates in in memory data structures
    long nanosReplay = System.nanoTime();
    while (command != null) {
      parsedCommand = parseToCommand(command);
      id = parsedCommand.getId();
      value = parsedCommand.getValue();
      action = parsedCommand.getAction();

      if ("insert".equalsIgnoreCase(action) || "update".equalsIgnoreCase(action)) {
        itemsMap.put(id, value);
      } else {
        itemsMap.remove(id);
      }
      command = tailer.readText();
    }
    System.out.println((System.nanoTime() - nanosReplay) / 1000 + " micros elapsed while replaying queue");

    // process incoming queries while continuing to update aggregates by consuming commands from
    // queue
    HttpServer server;
    int port = 8089;
    try {
      server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
    } catch (IOException ex){
      ex.printStackTrace();
      return;
    }
    ExecutorService executor = Executors.newSingleThreadExecutor();
    server.createContext("/item", new QueryHandler(itemsMap));
    server.setExecutor(executor);
    server.start();
    System.out.println("query server started at localhost:" + port);

    while (true) {
      command = tailer.readText();
      while (command != null) {
        parsedCommand = parseToCommand(command);
        id = parsedCommand.getId();
        value = parsedCommand.getValue();
        action = parsedCommand.getAction();

        if ("insert".equalsIgnoreCase(action) || "update".equalsIgnoreCase(action)) {
          itemsMap.put(id, value);
        } else {
          itemsMap.remove(id);
        }
        command = tailer.readText();
      }
    }
  }

  private static ParsedCommand parseToCommand(String command) {
    ParsedCommand parsedCommand = new ParsedCommand();
    String[] tokens = command.split("\\s+");
    parsedCommand.setAction(tokens[0]);
    parsedCommand.setId(Integer.parseInt(tokens[1]));
    if (tokens.length > 2) {
      parsedCommand.setValue(tokens[2]);
    }
    return parsedCommand;
  }
}
