package Utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ConvertTime {


    public static ZonedDateTime convertTime(String fileDateTime,String timeZone) {


        // read date time in custom format

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.DATEPATTERN);
        LocalDateTime date = LocalDateTime.parse(fileDateTime, formatter);

        // transform local date time in UTC format
        ZoneId utcZone = ZoneOffset.UTC;
        ZonedDateTime utcTime = ZonedDateTime.of(date,utcZone);

        // convert UTC datetime in ZoneID datetime
        return utcTime.withZoneSameInstant(ZoneId.of(timeZone));

    }

}
