
package com.mycompany.db2progetto;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class Neo4jAdministration implements AutoCloseable{
    private final Driver driver;
    String uri = "bolt://localhost:7687"; 
    String user = "neo4j"; 
    String password = "3647";

    public Neo4jAdministration(){
       this.uri = uri;
       this.user = user;
       this.password = password;
       driver = GraphDatabase.driver(this.uri, AuthTokens.basic(this.user, this.password));
    }
    
    @Override
    public void close() throws Exception{
        driver.close();
    }
    
    public boolean connectionNeo4j(){
        try(Session session = driver.session()) {
            Neo4jAdministration db = new Neo4jAdministration();
            System.out.println("\n*** CONNESSIONE STABILITA CON NEO4J ***\n");
            return true;   
        } catch(Exception e){
            System.out.println("\n*** Errore di connessione ***\n");
            return false;     
        }
    }
    
   
 // Query 1: Trova tutti i conti bancari posseduti da "Harli Haggidon"
    public void query1(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (c:BankAccount) - [:Conto_di] -> (p:Person{nome_cognome: 'Harli Haggidon'})\n" +
                                        "RETURN p, c");
            while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }

    public void esecuzioneQuery1(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (c:BankAccount) - [:Conto_di] -> (p:Person{nome_cognome: 'Harli Haggidon'})\n" +
                                        "RETURN p, c");
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }

    public void timeQuery1() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100_records/Query1";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/1000_records/Query1";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/10000_records/Query1";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100000_records/Query1";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 1 ---> Neo4j");
        /*** Eseguo la query 1 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery1();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query1();
        System.out.println("Fine query 1 ---> Neo4j\n");
        w.flush();
        w.close();
        close(); //driver.close
    }
    

 // Query 2: Trova tutte le società possedute da "Torie Pickersgill" con sede in Russia (RU)
    public void query2(){
        try(Session session = driver.session()){
            
            Result result = session.run("MATCH (p:Person{nome_cognome:'Torie Pickersgill'})- [:Possiede_quote] -> (s:Society {paese:'RU'})\n" +
                                        "RETURN p, s");
            while (result.hasNext())
            {
                
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }

    public void esecuzioneQuery2(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (p:Person{nome_cognome:'Torie Pickersgill'})- [:Possiede_quote] -> (s:Society {paese:'RU'})\n" +
                                        "RETURN p, s");
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }

    public void timeQuery2() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100_records/Query2";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/1000_records/Query2";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/10000_records/Query2";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100000_records/Query2";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 2 ---> Neo4j");
        /*** Eseguo la query 2 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery2();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query2();
        System.out.println("Fine query 2 ---> Neo4j\n");
        w.flush();
        w.close();
        close(); //driver.close
    }


 // Query 3: Trova tutte le società in cui "Rab Faulconer" possiede delle quote sociali superiori al 25%
    public void query3(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (p:Person{nome_cognome: 'Rab Faulconer'}) - [possesso:Possiede_quote] -> (s:Society)\n" +
                                        "WHERE possesso.quote > '25.0'\n" +
                                        "RETURN s, p");
            while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }
    
    public void esecuzioneQuery3(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (p:Person{nome_cognome: 'Rab Faulconer'}) - [possesso:Possiede_quote] -> (s:Society)\n" +
                                        "WHERE possesso.quote > '25.0'\n" +
                                        "RETURN s, p");
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }

    public void timeQuery3() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100_records/Query3";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/1000_records/Query3";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/10000_records/Query3";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100000_records/Query3";        
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 3 ---> Neo4j");
        /*** Eseguo la query 3 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery3();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query3();
        System.out.println("Fine query 3 ---> Neo4j\n");
        w.flush();
        w.close();
        close(); //driver.close
    }   
    

 // Query 4: Trova la persona di nome "Dominick Condon" che ha effettuato delle transazioni 
 //          con un importo > di 45000 euro sul conto corrente di "Les Rodders"
    public void query4(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (p1:Person{nome_cognome: 'Dominick Condon'}) - [soldi:P_effettua_t] -> (c:BankAccount), \n" +
                                              "(c:BankAccount) - [:Conto_di] -> (p2:Person{nome_cognome: 'Les Rodders'})\n" +
                                        "WHERE soldi.importo>'€45000'\n" +
                                        "RETURN c, p1, p2");
            while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }

    public void esecuzioneQuery4(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (p1:Person{nome_cognome: 'Dominick Condon'}) - [soldi:P_effettua_t] -> (c:BankAccount), \n" +
                                              "(c:BankAccount) - [:Conto_di] -> (p2:Person{nome_cognome: 'Les Rodders'})\n" +
                                        "WHERE soldi.importo>'€45000'\n" +
                                        "RETURN c, p1, p2");
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }
 
    public void timeQuery4() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100_records/Query4";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/1000_records/Query4";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/10000_records/Query4";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100000_records/Query4";        
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 4 ---> Neo4j");
        /*** Eseguo la query 4 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery4();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query4();
        System.out.println("Fine query 4 ---> Neo4j\n");
        w.flush();
        w.close();
        close(); //driver.close
    }
    

 // Query 5: Trova la società che ha effettuato delle transazioni il 15 Gennaio del 2044 sul conto corrente 
 //          posseduto da "Dorotea Menure" che è parente con delle persone che possiedono quote sociali 
 //          maggiori al 25% riguardanti la societa stessa
    public void query5(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (s:Society) - [data:S_effettua_t] -> (c:BankAccount), \n" +
                                              "(c:BankAccount) - [:Conto_di] -> (p1:Person{nome_cognome: 'Dorotea Menure'}), \n" +
                                              "(p1:Person) - [:Parente_con] -> (p2:Person), \n" +
                                              "(p2:Person) - [possiede:Possiede_quote] -> (s:Society)\n" +
                                       "WHERE data.data_transazione = '15/01/2044' AND possiede.quote > '25.0'\n" +
                                       "RETURN s, c, p1, p2");
            while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }

    public void esecuzioneQuery5(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (s:Society) - [data:S_effettua_t] -> (c:BankAccount), \n" +
                                              "(c:BankAccount) - [:Conto_di] -> (p1:Person{nome_cognome: 'Dorotea Menure'}), \n" +
                                              "(p1:Person) - [:Parente_con] -> (p2:Person), \n" +
                                              "(p2:Person) - [possiede:Possiede_quote] -> (s:Society)\n" +
                                       "WHERE data.data_transazione = '15/01/2044' AND possiede.quote > '25.0'\n" +
                                       "RETURN s, c, p1, p2");
        }catch(Exception ex){
            System.out.println("query non eseguita");
        }
    }

    public void timeQuery5() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100_records/Query5";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/1000_records/Query5";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/10000_records/Query5";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/Neo4j/100000_records/Query5";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 5 ---> Neo4j");
        /*** Eseguo la query 5 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery5();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query5();
        System.out.println("Fine query 5 ---> Neo4j\n");
        w.flush();
        w.close();
        close(); //driver.close
    }

}
 
