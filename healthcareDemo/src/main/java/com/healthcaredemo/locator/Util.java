package com.healthcaredemo.locator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class Util {
  public static String yesterday() {
    LocalDateTime yesterday = LocalDateTime.now();
    yesterday = yesterday.truncatedTo(ChronoUnit.DAYS);
    yesterday = yesterday.minusDays(1l);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    return yesterday.format(formatter);
  }
}
