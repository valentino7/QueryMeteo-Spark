package first_query;

import java.io.Serializable;
import java.util.ArrayList;

public class Record implements Serializable {

    private ArrayList<Integer> city;
    private String date;
    private String hour;
    private ArrayList<String> description;

    public Record() {
        this.city = new ArrayList<>();
        this.description = new ArrayList<>();
    }

    public ArrayList<Integer> getCity() {
        return city;
    }

    public void setCity(ArrayList<Integer> city) {
        this.city = city;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public ArrayList<String> getDescription() {
        return description;
    }

    public void setDescription(ArrayList<String> description) {
        this.description = description;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
