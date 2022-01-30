package net.openhft.chronicle.queue.simple.input.domain;

import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;

import java.util.Arrays;

public class CommandValidator {
  private static final String[] VALID_ACTIONS = {Command.UPSERT, Command.DELETE};

  public static boolean isValid(CommandDTO dto) {
    if (Arrays.stream(VALID_ACTIONS).noneMatch(valid -> valid.equalsIgnoreCase(dto.getAction()))) {
      return false;
    }
    try {
      if (dto.getId() < 0) {
        throw new NumberFormatException("Cannot be a negative id");
      }
    } catch (NumberFormatException ex) {
      System.err.println(ex.getMessage());
      return false;
    }
    if (dto.getValue() != null && dto.getValue().length() > 512) {
      System.err.println("Value must be 512 characters or less.");
      return false;
    }
    return true;
  }
}
