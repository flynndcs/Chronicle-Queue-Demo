package net.openhft.chronicle.queue.simple.input.domain;

import java.util.Arrays;

public class CommandValidator {
  private static final String[] validActions = {
          ParsedCommand.INSERT, ParsedCommand.UPDATE, ParsedCommand.DELETE
  };

  // command must be in form "{insert|update|delete} {int} {string with len 0-512}"
  public static boolean isValid(String command) {
    String[] tokens = command.split("\\s+");
    if (Arrays.stream(validActions).noneMatch(tokens[0]::equalsIgnoreCase)) {
      return false;
    }
    try {
      int parsedInt = Integer.parseInt(tokens[1]);
      if (parsedInt < 0) {
        throw new NumberFormatException("Cannot be a negative id");
      }
    } catch (NumberFormatException ex) {
      System.err.println(ex.getMessage());
      return false;
    }
    if (tokens.length > 2 && tokens[2].length() > 512) {
      System.err.println("Value must be 512 characters or less.");
      return false;
    }
    return true;
  }
}
