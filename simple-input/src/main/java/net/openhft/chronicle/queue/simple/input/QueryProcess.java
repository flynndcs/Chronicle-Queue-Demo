package net.openhft.chronicle.queue.simple.input;

import com.sun.net.httpserver.HttpServer;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.simple.input.domain.Command;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;
import net.openhft.chronicle.queue.simple.input.http.QueryHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/**
 * Process implementing read model for queries to items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class QueryProcess {
  private static final String QUEUE = "queue";
  private static final String UPSERT = "upsert";
  private static final String ITEM_PATH = "/item";
  private static final String HANDLE_NEW_COMMAND = "handling new command";
  private static final String REPLAY_TIME_MESSAGE = " micros elapsed while replaying commands";
  private static final String REPLAY_COUNT_MESSAGE = " commands replayed";
  private static final String AGGREGATES_PRESENT = " aggregates present";
  private static final String HOSTNAME = "localhost";
  private static final String SERVER_START_MESSAGE = "query server started at localhost: ";
  private static final int PORT = 8089;

  private static final Map<Long, String> ITEMS_MAP = new HashMap<>();

  private static final QueryHandler QUERY_HANDLER = new QueryHandler(ITEMS_MAP);
  private static final ExcerptTailer TAILER =
      binary(QUEUE).build().createTailer();

  private static final Command COMMAND = new Command();
  private static CommandDTO COMMAND_DTO = new CommandDTO();

  private static long NANOS_START = System.nanoTime();
  private static boolean READ = false;

  public static void main(String[] args) throws IOException {
    replayCommands();
    startServer(getServer());
    handleCommands();
  }

  private static Command parseToCommand(CommandDTO dto) {
    COMMAND.setAction(dto.getAction());
    COMMAND.setId(dto.getId());
    if (dto.getValue() != null) {
      COMMAND.setValue(dto.getValue());
    }
    return COMMAND;
  }

  private static void replayCommands() {
    NANOS_START = System.nanoTime();
    int commandsReplayed = 0;
    while (didReadAndSetNewCommandDTO()) {
      commandsReplayed++;
      handleCommand(parseToCommand(COMMAND_DTO));
    }
    System.out.println((System.nanoTime() - NANOS_START) / 1000 + REPLAY_TIME_MESSAGE);
    System.out.println(commandsReplayed + REPLAY_COUNT_MESSAGE);
    System.out.println(ITEMS_MAP.size() + AGGREGATES_PRESENT);
  }

  private static void startServer(HttpServer server) {
    server.createContext(ITEM_PATH, QUERY_HANDLER);
    server.setExecutor(newSingleThreadExecutor());
    server.start();
    System.out.println(SERVER_START_MESSAGE + PORT);
  }

  private static HttpServer getServer() throws IOException {
    return HttpServer.create(new InetSocketAddress(HOSTNAME, PORT), 0);
  }

  private static void handleCommands() {
    while (true) {
      if (didReadAndSetNewCommandDTO()) {
        handleCommand(parseToCommand(COMMAND_DTO));
      }
    }
  }

  private static void handleCommand(Command parsedCommand) {
    System.out.println(HANDLE_NEW_COMMAND);

    // business logic here, placed into in-memory data structure
    // command -> data to be read
    if (UPSERT.equalsIgnoreCase(parsedCommand.getAction())) {
      ITEMS_MAP.put(parsedCommand.getId(), parsedCommand.getValue());
    } else {
      ITEMS_MAP.remove(parsedCommand.getId());
    }
  }

  private static boolean didReadAndSetNewCommandDTO() {
    return TAILER.readDocument(wire -> COMMAND_DTO.readMarshallable(wire));
  }
}
