package com.pszymczyk.app8;

import java.time.LocalDate;
import java.util.Map;

public record DailyTransactionsLog(Map<LocalDate, Integer> log) {
}
