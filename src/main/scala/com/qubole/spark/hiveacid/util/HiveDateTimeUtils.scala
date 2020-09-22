package com.qubole.spark.hiveacid.util

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}

import com.qubole.shaded.hadoop.hive.common.`type`.{Date => HiveDate, Timestamp => HiveTimestamp}
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf

object HiveDateTimeUtils {

  private final val gregorianStartDate = LocalDate.of(1582, 10, 15)
  private final val julianEndDate = LocalDate.of(1582, 10, 4)

  private final val gregorianStartTs = LocalDateTime.of(gregorianStartDate, LocalTime.MIDNIGHT)
  private final val julianEndTs = LocalDateTime.of(
    julianEndDate,
    LocalTime.of(23, 59, 59, 999999999))

  private val gregorianStartSQLDate = gregorianStartDate.toEpochDay.toInt
  private val julianEndSQLDate = julianEndDate.toEpochDay.toInt

  /**
    * Returns the number of days since epoch from HiveDate.
    */
  def fromHiveDate(date: HiveDate): SQLDate = {
    date.toEpochDay
  }

  def toHiveDate(days: SQLDate): HiveDate = {
    val shiftedDays =
      if (julianEndSQLDate < days && days < gregorianStartSQLDate) {
      gregorianStartSQLDate
    } else {
      days
    }
    HiveDate.ofEpochDay(shiftedDays)
  }

  /**
    * Returns a HiveTimestamp from number of micros since epoch.
    */
  def toHiveTimestamp(micros: SQLTimestamp): HiveTimestamp = {
    val instant = microsToInstant(micros)
    var ldt = LocalDateTime.ofInstant(instant, getZoneId(SQLConf.get.sessionLocalTimeZone))
    if (ldt.isAfter(julianEndTs) && ldt.isBefore(gregorianStartTs)) {
      ldt = LocalDateTime.of(gregorianStartDate, ldt.toLocalTime)
    }
    HiveTimestamp.ofEpochSecond(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano)
  }

  /**
    * Returns the number of micros since epoch from HiveTimestamp.
    */
  def fromHiveTimestamp(timestamp: HiveTimestamp): SQLTimestamp = {
    val ldt =
      LocalDateTime.ofEpochSecond(timestamp.toEpochSecond, timestamp.getNanos, ZoneOffset.UTC)
    val instant = ldt.atZone(getZoneId(SQLConf.get.sessionLocalTimeZone)).toInstant
    instantToMicros(instant)
  }
}