package first_query;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;

public class RecordParser {

    private ArrayList<String> cities;

    public RecordParser(String strCities){
        this.cities = new ArrayList<>();
        String[] splitCity = strCities.split(",");
        for (int i = 1; i < splitCity.length; i++){
            this.cities.add(splitCity[i]);
        }
        System.out.println("constructor"+this.cities);
    }

    public Record parseCSV(String csvLine ) {

        Record record = null;
        String[] csvValues = csvLine.split(",");
        record = new Record();

        HashMap<String,String> map = new HashMap<>();
        for (int i = 0; i < csvValues.length; i++){
            if ( i == 0) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
                LocalDate date = LocalDate.parse(csvValues[i], formatter);
                record.setDate(date);
            }
            else {
                map.put(this.cities.get(i-1),csvValues[i]);
            }
        }

        record.setWeather_city(map);


        return record;
    }
}
