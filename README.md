# spark_project


File: 

  * __city attributes.csv__: per ogni città, viene riportata la latitudine e la longitudine;

  * __humidity.csv__: per ogni città, viene riportata l’umidità (in %) registrata in una specifica ora di uno
  specifico giorno di un dato anno del dataset;

  * __pressure.csv__: per ogni città, viene riportata la pressione (in hPa) registrata in una specifica ora
  di uno specifico giorno di un dato anno del dataset;

  * __temperature.csv__: per ogni città, viene riportata la temperatura (in gradi Kelvin) registrata in una
  specifica ora di uno specifico giorno di un dato anno del dataset;

  * __weather description.csv__: per ogni città viene riportata la descrizione (espressa in formato
  String) delle condizioni meteo in una specifica ora, in uno specifico giorno di un dato anno del
  dataset. Ad esempio, una giornata serena viene descritta con la stringa sky is clear.

Query: 

  * 1 Per ogni anno del dataset individuare le città che hanno almeno 15 giorni al mese di tempo sereno nei
  mesi di marzo, aprile e maggio.

  * 2 Individuare, per ogni nazione, la media, la deviazione standard, il minimo, il massimo della tempera-
  tura, della pressione e dell’umidità registrata in ogni mese di ogni anno.
  Nota: la nazione a cui appartiene ogni città non viene indicata in modo esplicito nel dataset, ma deve
  essere ricavata.

  * 3 Individuare, per ogni nazione, le 3 città che hanno registrato nel 2017 la massima differenza di tem-
  perature medie nella fascia oraria locale 12:00-15:00 nei mesi di giugno, luglio, agosto e settembre ri-
  spetto ai mesi di gennaio, febbraio, marzo e aprile. Confrontare la posizione delle città nella classifica
  dell’anno precedente (2016).
