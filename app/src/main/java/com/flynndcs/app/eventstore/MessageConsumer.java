package com.flynndcs.app.eventstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flynndcs.app.dto.CommandDTO;

import java.sql.SQLException;

public interface MessageConsumer {
  void onCommand(CommandDTO dto) throws JsonProcessingException, SQLException;
}
