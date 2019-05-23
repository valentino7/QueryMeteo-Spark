# spark_project


File (in Data directory): 

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
  
  
Prerequisiti:

    python 2.7
    docker
    docker-compose
    java 8
    maven
    
Avvio:

    modificare il pom nel seguente modo se si vuole avviare lo script start-all con l'argomento --submit: 
         <properties>
                <framework.scope>compile</framework.scope>
         </properties>
         andrà sostituito con 
         <properties>
                <framework.scope>provided</framework.scope>
         </properties>
         quando viene generato il jar con il comando mvn clean package se è presente 
             compile:  tutte le librerie nel pom che hanno questo scope veranno inserire nel jar
             provided:  tutte le librerie nel pom che hanno questo scope non veranno inserire nel jar
    
    mvn clean package
    cd src/main/java/docker/
    ./start-all.sh
    ARGOMENTI:
            --submit : effettua il deploy del jar sul cluster spark
    senza l'argomento --submit non verranno effettuato il submit delle query sul cluster di spark

    
start all avvia automaticamente i seguenti script:
   * start-hdfs.sh : avvia 4 container di hdfs 1 master e 3 worker
   * nifi-run.sh : avvia un container con apache-nifi
   * start-hbase.sh : avvia 1 container con apache-hbase
   * init-db.sh : crea le tabelle dentro hbase per le 3 query
   * mongo-server-start.sh : avvia un container con mongo db
   * spark-run.sh : avvia tramite docker-compose 3 container: un master e 2 worker di spark
   *activate_processo_nifi.py : script in python per attivare automaticamente l'injection dei file di input nell'hdfs tramite nifi
       e per riempire le tabelle dei db una volta terminata l'esecuzione delle query
       
    
    