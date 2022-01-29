package net.openhft.chronicle.queue.simple.input;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.simple.input.domain.CommandValidator;

import java.util.Scanner;

/**
 * Command path for items application.
 *
 * @author Daniel Flynn (dflynn)
 */
public class CommandProcess {
  public static void main(String[] args) {
    String path = "queue";
    SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
    ExcerptAppender appender = queue.acquireAppender();
    Scanner read = new Scanner(System.in);
    while (true) {
      System.out.println("command: ");
      String line = read.nextLine();
      if (line.isEmpty()) {
        continue;
      }
      if ("exit".equalsIgnoreCase(line)) {
        break;
      } else if (!CommandValidator.isValid(line)) {
        System.err.println(
            "invalid command - must be in form '{insert|update|delete} {int} {string with len 0-512}'");
      } else {
        appender.writeText(line);
      }
    }
    System.out.println("... bye.");
  }




}
