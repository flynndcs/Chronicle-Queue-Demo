package net.openhft.chronicle.queue.simple.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/** Created by catherine on 18/07/2016. */
public class CommandPersister {
  private static String DB_HOSTNAME;
  private static String pgUrl = "";
  private static String pgUser = "postgres";
  private static String pgPass = "postgres";

  private static MessageConsumer persister;
  private static SingleChronicleQueue queue;
  private static MethodReader reader;
  private static Connection connection;
  private static PreparedStatement statement;
  private static final ObjectWriter WRITER = new ObjectMapper().writerFor(CommandDTO.class);

  public static void main(String[] args) throws SQLException{
    if (args.length > 0 && args[0] != null) {
      DB_HOSTNAME = args[0];
    }

    System.out.println("DB Hostname: " + DB_HOSTNAME);
    pgUrl = "jdbc:postgresql://" + DB_HOSTNAME + ":5432/postgres";
    connection = DriverManager.getConnection(pgUrl, pgUser, pgPass);
    statement = connection.prepareStatement("INSERT INTO event (event, written_at) VALUES (?, ?)");
    statement.setQueryTimeout(5);
    queue = binary("queue").build();
    persister = new MessagePersister(statement, WRITER);
    reader = queue.createTailer().methodReader(persister);
    while (true) {
      if (reader.readOne()) {
        System.out.println("event persisted");
      } else {
        Jvm.pause(50);
      }
    }
  }
}
