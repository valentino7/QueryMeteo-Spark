package sparkSQL.model;

import java.io.Serializable;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class WeatherDescription implements Serializable {

    private GregorianCalendar date;
    private int city;
    private String description;
    private Double temperature;
    private double humidity;


    public GregorianCalendar getDate() {
        return date;
    }

    public void setDate(GregorianCalendar date) {
        this.date = date;
    }

    public int getCity() {
        return city;
    }

    public void setCity(int city) {
        this.city = city;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "Anno: " +this.getDate().get(Calendar.YEAR) +
                "\t" + "Mese: " + this.getDate().get(Calendar.MONTH)  +
                "\t" + "Giorno: "+ this.getDate().get(Calendar.DAY_OF_MONTH) +
                "\t" + this.getCity()+
                "\t" + this. getDescription();
    }
}
