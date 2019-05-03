package first_query;

import java.util.ArrayList;

public class RecordParser {

    public static Record parseCSV(String csvLine ) {

        Record record = null;
        String[] csvValues = csvLine.split(",");
        record = new Record();

        ArrayList<Integer> tempCity = new ArrayList<>();
        ArrayList<String> tempDescription = new ArrayList<>();
        String[] d;
        for (int i = 0; i < csvValues.length; i++){
            if ( i == 0) {
                d = csvValues[i].split(" ");
                record.setDate(d[0]);
                record.setHour(d[1]);
            }
            else {
                tempCity.add(i);
                tempDescription.add(csvValues[i]);
            }
        }

        record.setCity(tempCity);
        record.setDescription(tempDescription);



        return record;
    }
}
