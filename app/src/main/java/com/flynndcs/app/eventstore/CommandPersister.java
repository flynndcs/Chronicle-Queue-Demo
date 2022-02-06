package com.flynndcs.app.eventstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flynndcs.app.dto.CommandDTO;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Created by catherine on 18/07/2016. */
public class CommandPersister implements Runnable {
  private static MessageConsumer persister;
  private static MethodReader reader;
  private static ExcerptTailer tailer;
  private static boolean printed;
  private static final ObjectWriter WRITER = new ObjectMapper().writerFor(CommandDTO.class);

  public CommandPersister(String dbHost, SingleChronicleQueue queue) throws SQLException {
    Connection connection =
        DriverManager.getConnection(
            "jdbc:postgresql://" + dbHost + ":5432/postgres", "postgres", "postgres");
    persister = new EventPersister(connection, WRITER);
    reader = queue.createTailer().toEnd().methodReader(persister);
  }

  @Override
  public void run() {
    while (true) {
      printed = false;
      while (reader.readOne()) {
        if (!printed) {
          System.out.println("Persisting to event store");
          printed = true;
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
