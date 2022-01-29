package net.openhft.chronicle.queue.simple.input;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.simple.input.domain.ParsedCommand;
import net.openhft.chronicle.queue.simple.input.domain.ParsedQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Process implementing read model for queries to items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class QueryProcess {
  public static void main(String[] args) {
    // get queue from beginning
    String path = "queue";
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

    // process incoming queries while continuing to update aggregates by consuming commands from
    // queue
    Scanner read = new Scanner(System.in);
    while (true) {
      System.out.println("query:");
      // blocking wait for user input
      String query = read.nextLine();
      long nanos = System.nanoTime();
      // before handling query from user input, check for commands that could influence evaluation
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

      // when all commands in queue handled do we respond to queries to ensure state correctness
      if (query == null) {
        Jvm.pause(10);
      } else if ("exit".equalsIgnoreCase(query)) {
        return;
      } else {
        ParsedQuery parsedQuery = parseToQuery(query);
        if (parsedQuery != null) {
          // execute query against in memory embedded database and in memory map
          String mapValue = itemsMap.get(parsedQuery.getId());
          if (mapValue == null) {
            System.out.println("Value not found.");
            System.out.println((System.nanoTime() - nanos) / 1000 + " micros elapsed");
          } else {
            System.out.println("Value: " + mapValue);
            System.out.println((System.nanoTime() - nanos) / 1000 + " micros elapsed");
          }
        }
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

  private static ParsedQuery parseToQuery(String query) {
    ParsedQuery parsedQuery = new ParsedQuery();
    try {
      parsedQuery.setId(Integer.parseInt(query));
    } catch (NumberFormatException ex) {
      System.err.println("Invalid query, must be an integer");
      return null;
    }
    return parsedQuery;
  }
}
