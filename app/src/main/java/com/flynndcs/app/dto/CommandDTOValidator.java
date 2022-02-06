package com.flynndcs.app.dto;

import java.util.Arrays;

public class CommandDTOValidator {
  private static final String[] VALID_ACTIONS = {"ADD", "SUBTRACT"};

  public static boolean isValid(CommandDTO dto) {
    if (Arrays.stream(VALID_ACTIONS).noneMatch(valid -> valid.equalsIgnoreCase(dto.getAction()))) {
      return false;
    }
    if (dto.getQuantity() < 1) {
      System.err.println("Value must be positive and nonzero.");
      return false;
      }
    return true;
    }
  }
