package net.openhft.chronicle.queue.simple.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;

import java.sql.SQLException;

public interface MessageConsumer {
  void onMessage(CommandDTO dto) throws JsonProcessingException, SQLException;

}
