package com.example;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.java.tuple.Tuple3;

public class SensorData implements Serializable {
    public String name;

    public long timestamp;
    public double value;
    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");


    public SensorData() {
    }

    public SensorData(Tuple3<String, Long, Double> msg) {
        this.name = msg.f0;
        this.timestamp = msg.f1;
        this.value = msg.f2;
    }
    public SensorData(String msg) throws Exception {
        this(parseStringToPojo(msg));
    }

    //    public SensorMessage (String message) throws Exception {
//        SensorMessage(name, date, val);
    public static Tuple3<String, Long, Double> parseStringToPojo(String message) throws Exception {
        // message comes as ('name', 'date', val)
        Pattern pattern = Pattern.compile("\\('([\\w]+)', '([\\d \\-\\:]+)', ([\\d\\.\\-]+)\\)");
        Matcher matcher = pattern.matcher(message);
        matcher.find();

        String name; long timestamp; double value;

        try {
            name = matcher.group(1);

            timestamp = simpleDateFormat.parse(matcher.group(2)).getTime();
            value = Double.parseDouble(matcher.group(3));

        } catch (IllegalStateException | ParseException ise) {
            throw new Exception(ise);
        }

        return new Tuple3<>(name, timestamp, value);
    }

    @Override
    public String toString() {
        return "{{" + this.name + ", " + simpleDateFormat.format(new Date(this.timestamp)) + ", " + this.value + "}}";
    }
    public SensorData WithTimestampEndOfDay() {
        return new SensorData(Tuple3.of(this.name, getMessageTimeOnlyDay(), this.value));
    }

    public long getMessageTimeOnlyDay() {
        return this.timestamp - this.timestamp % (24 * 60 * 60 * 1000);
    }
//    public byte[] getBytes() {
//        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(bos)) {
//            oos.writeObject(this);
//            oos.flush();
//            return bos.toByteArray();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
