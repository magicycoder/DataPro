package com.cy.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateTemperaturePair
        implements Writable, WritableComparable<DateTemperaturePair> {

    private Text yearMonth = new Text();
    private Text day = new Text();
    private IntWritable temperature = new IntWritable();

    public DateTemperaturePair() {

    }

    public DateTemperaturePair(String yearMonth, String day, int temperture) {
        this.yearMonth.set(yearMonth);
        this.day.set(day);
        this.temperature.set(temperture);
    }

    public static DateTemperaturePair read(DataInput in) throws IOException {
        DateTemperaturePair pair = new DateTemperaturePair();
        pair.readFields(in);
        return pair;
    }

    public void write(DataOutput out) throws IOException {
        yearMonth.write(out);
        day.write(out);
        temperature.write(out);
    }


    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        day.readFields(in);
        temperature.readFields(in);
    }

    public Text getYearMonthDay() {
        return new Text(yearMonth.toString() + day.toString());
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public Text getDay() {
        return day;
    }

    public void setYearMonth(String yearMonthAsString) {
        yearMonth.set(yearMonthAsString);
    }

    public void setTemperature(int temp) {
        temperature.set(temp);
    }

    public void setDay(String dayAsString) {
        day.set(dayAsString);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateTemperaturePair that = (DateTemperaturePair) o;
        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
            return false;
        }
        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
            return false;
        }
        return true;

    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = result + 31 + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    public int compareTo(DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(pair.getTemperature());
        }
        return -1*compareValue;
    }

    @Override
    public String toString() {
        StringBuilder sbr = new StringBuilder();
        sbr.append("DateTemperturePair{yearMonth=");
        sbr.append(yearMonth);
        sbr.append(", day=");
        sbr.append(day);
        sbr.append(", temperature=");
        sbr.append(temperature);
        sbr.append("}");
        return sbr.toString();
    }
}