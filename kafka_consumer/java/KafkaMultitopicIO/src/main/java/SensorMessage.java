import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SensorMessage implements Serializable {
    public String name;

    public long getTimestamp() {
        return timestamp;
    }

    public long timestamp;
    public double value;

    public SensorMessage() {
    }

    public SensorMessage(String name, long timestamp, double value) {
        this.name = name;
        this.timestamp = timestamp;
        this.value = value;
    }

    public SensorMessage (String message) throws ParseException {
        // message comes as ('name', 'date', val)
        Pattern pattern = Pattern.compile("\\('([\\w]+)', '([\\d \\-\\:]+)', ([\\d\\.\\-]+)\\)");
        Matcher matcher = pattern.matcher(message);
        matcher.find();

        String name = matcher.group(1);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date msgTime = simpleDateFormat.parse(matcher.group(2));

        double value = Double.parseDouble(matcher.group(3));

        this.name = name;
        this.timestamp = msgTime.getTime();
        this.value = value;
    }

    @Override
    public String toString() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC+2"));
        return "{{" + this.name + ", " + new Date(this.timestamp).toString() + ", " + this.value + "}}";
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
