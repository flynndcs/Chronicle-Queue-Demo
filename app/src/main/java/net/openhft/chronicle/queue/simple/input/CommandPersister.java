package net.openhft.chronicle.queue.simple.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flynndcs.app.dto.CommandDTO;
import com.flynndcs.app.eventstore.MessageConsumer;
import com.flynndcs.app.eventstore.MessagePersister;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/** Created by catherine on 18/07/2016. */
public class CommandPersister implements Runnable {
  private static String DB_HOSTNAME;

  private static MessageConsumer persister;
  private static SingleChronicleQueue queue;
  private static MethodReader reader;
  private static PreparedStatement statement;
  private static final ObjectWriter WRITER = new ObjectMapper().writerFor(CommandDTO.class);
  private static boolean printed = false;

  public CommandPersister(String dbHost) {
    DB_HOSTNAME = dbHost;
    System.out.println("DB Hostname: " + DB_HOSTNAME);
    queue = binary("queue").build();
    try {
      statement =
          DriverManager.getConnection(
                  "jdbc:postgresql://" + DB_HOSTNAME + ":5432/postgres", "postgres", "postgres")
              .prepareStatement("INSERT INTO event (event_id, event, written_at) VALUES (?, ?, ?)");
      statement.setQueryTimeout(5);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    persister = new MessagePersister(statement, WRITER);
    reader = queue.createTailer().methodReader(persister);
  }

  @Override
  public void run() {
    printed = false;
    while (true) {
      if (reader.readOne() && !printed) {
        printed = true;
        System.out.println("Persisting to event store");
      } else {
        break;
      }
    }
  }
}
