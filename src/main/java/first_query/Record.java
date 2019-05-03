package first_query;


import java.io.Serializable;
import java.time.LocalDate;

import java.util.HashMap;


public class Record implements Serializable {

    private LocalDate date;
    private HashMap<Integer,String> weather_city; //città, descrizione


    public Record() {

    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public HashMap<Integer, String> getWeather_city() {
        return weather_city;
    }

    public void setWeather_city(HashMap<Integer, String> weather_city) {
        this.weather_city = weather_city;
    }

    @Override
    public String toString() {
        return this.date.toString() + "\t" + this.weather_city.toString();
    }
}
