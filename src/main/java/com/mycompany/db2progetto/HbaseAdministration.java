
package com.mycompany.db2progetto;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseAdministration {
    
    //static String fileName = "./dataset/100_records.csv"; 
    //static String fileName = "./dataset/1000_records.csv"; 
    //static String fileName = "./dataset/10000_records.csv"; 
    static String fileName = "./dataset/100000_records.csv"; 
    
    protected static String MY_NAMESPACE_NAME = "HbaseBenchmark"; 
    //static TableName MY_TABLE_NAME = TableName.valueOf("db1"); // dataset da 100
    //static TableName MY_TABLE_NAME = TableName.valueOf("db2"); // dataset da 1000
    //static TableName MY_TABLE_NAME = TableName.valueOf("db3"); // dataset da 10000
    static TableName MY_TABLE_NAME = TableName.valueOf("db4"); // dataset da 100000
    static Table table;
    
    static byte[] MY_COLUMN_FAMILY_NAME_PERSON = Bytes.toBytes("Person");
    static byte[] MY_COLUMN_FAMILY_NAME_SOCIETY = Bytes.toBytes("Society");
    static byte[] MY_COLUMN_FAMILY_NAME_BANKACCOUNT = Bytes.toBytes("BankAccount");
    
    static String[]columnPerson = {"id"/*row key*/, "id_conto_P1", "nome", "cognome", "nome_cognomeP1", "tel", "email", "indirizzo"};
    static String[]columnSociety = {"id"/*row key*/, "id_societa2", "nome_societa", "paese", "citta", "quote"};
    static String[]columnBankAccount = {"id"/*row key*/, "id_conto_P2", "id_credito_banca", "nome_cognomeP2", "professione", "tipo_tessera", "data_transazione", "data_apertura", "importo"};
    static int input;
    HbaseDB hbase = new HbaseDB();
    
    public HbaseAdministration() throws IOException{
        this.fileName = fileName;
        this.MY_NAMESPACE_NAME = MY_NAMESPACE_NAME;
        this.MY_TABLE_NAME = MY_TABLE_NAME;
        this.MY_COLUMN_FAMILY_NAME_PERSON = MY_COLUMN_FAMILY_NAME_PERSON;
        this.MY_COLUMN_FAMILY_NAME_SOCIETY = MY_COLUMN_FAMILY_NAME_SOCIETY;
        this.MY_COLUMN_FAMILY_NAME_BANKACCOUNT = MY_COLUMN_FAMILY_NAME_BANKACCOUNT;
        this.columnPerson = columnPerson;
        this.columnSociety = columnSociety;
        this.columnBankAccount = columnBankAccount;
        this.table = table;
    }
    
    public boolean connectionHBase() throws IOException{
        try{
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            System.out.println("\n*** CONNESSIONE STABILITA CON HBASE ***\n");
            hbase.createNamespaceAndTable(admin,MY_NAMESPACE_NAME, MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME_PERSON,
                    MY_COLUMN_FAMILY_NAME_SOCIETY,MY_COLUMN_FAMILY_NAME_BANKACCOUNT);
            this.table = connection.getTable (MY_TABLE_NAME);
            return true;
        }
        catch(IOException e){
            return false;
        } 
    } 
    
    
    void putValuePerson(){ 
        hbase.importLocalFileToHBase(this.fileName, this.table, this.MY_COLUMN_FAMILY_NAME_PERSON, this.columnPerson);
    }
    void putValueSociety(){ 
        hbase.importLocalFileToHBase(this.fileName, this.table, this.MY_COLUMN_FAMILY_NAME_SOCIETY, this.columnSociety);
    }
    void putValueBankAccount(){ 
        hbase.importLocalFileToHBase(this.fileName, this.table, this.MY_COLUMN_FAMILY_NAME_BANKACCOUNT, this.columnBankAccount);
    }
    
 // Query 1: Trova tutti i conti bancari posseduti da 'Harli Haggidon'
    public void query1() throws IOException{
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT,
            Bytes.toBytes("nome_cognomeP2"), CompareOp.EQUAL, Bytes.toBytes("Harli Haggidon"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        //stampo il risultato di scan
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }
    }

    public void esecuzioneQuery1() throws IOException{
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT,
            Bytes.toBytes("nome_cognomeP2"), CompareOp.EQUAL, Bytes.toBytes("Harli Haggidon"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
    }
     
    public void timeQuery1() throws IOException{
        double millisecondi = Math.pow(10, 6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100_records/Query1";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/1000_records/Query1";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/10000_records/Query1";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100000_records/Query1";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 1 ---> Hbase ");
        /*** Eseguo la query 1 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery1();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query1();
        System.out.println("Fine query 1 ---> Hbase\n");
        w.flush();
        w.close();  
    }
    
    
 // Query 2: Trova tutte le società possedute da "Torie Pickersgill" con sede in Russia (RU)
    public void query2() throws IOException{
        List<Filter> filters = new ArrayList<>();
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_PERSON,
            Bytes.toBytes("nome_cognomeP1"), CompareOp.EQUAL, Bytes.toBytes("Torie Pickersgill"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_SOCIETY,
            Bytes.toBytes("paese"), CompareOp.EQUAL, Bytes.toBytes("RU"));
        filters.add(filter2);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        scan.setFilter(filterList);
        ResultScanner scanner = HbaseAdministration.table.getScanner(scan);
        //stampo il risultato di scan
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        } 
    }

    public void esecuzioneQuery2() throws IOException{
        List<Filter> filters = new ArrayList<>();
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_PERSON,
            Bytes.toBytes("nome_cognomeP1"), CompareOp.EQUAL, Bytes.toBytes("Torie Pickersgill"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_SOCIETY,
            Bytes.toBytes("paese"), CompareOp.EQUAL, Bytes.toBytes("RU"));
        filters.add(filter2);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        scan.setFilter(filterList);
        ResultScanner scanner = HbaseAdministration.table.getScanner(scan);
    }
     
    public void timeQuery2() throws IOException{
        double millisecondi = Math.pow(10, 6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100_records/Query2";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/1000_records/Query2";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/10000_records/Query2";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100000_records/Query2";        
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 2 ---> Hbase");
        /*** Eseguo la query 2 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime(); 
            esecuzioneQuery2();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query2();
        System.out.println("Fine query 2 ---> Hbase\n");
        w.flush();
        w.close();  
    }
    
 // Query 3: Trova tutte le società in cui "Rab Faulconer" possiede delle quote sociali superiori al 25%
    public void query3() throws IOException{
        List<Filter> filters = new ArrayList<>();
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_PERSON,
            Bytes.toBytes("nome_cognomeP1"), CompareOp.EQUAL, Bytes.toBytes("Rab Faulconer"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_SOCIETY,
            Bytes.toBytes("quote"), CompareOp.GREATER, Bytes.toBytes("25.0"));
        filters.add(filter2);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters); 
        scan.setFilter(filterList);
        ResultScanner scanner = HbaseAdministration.table.getScanner(scan);
        //stampo il risultato di scan
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }
    }
     
    public void esecuzioneQuery3() throws IOException{
        List<Filter> filters = new ArrayList<>();
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_PERSON,
            Bytes.toBytes("nome_cognomeP1"), CompareOp.EQUAL, Bytes.toBytes("Rab Faulconer"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_SOCIETY,
            Bytes.toBytes("quote"), CompareOp.GREATER, Bytes.toBytes("25.0"));
        filters.add(filter2);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters); 
        scan.setFilter(filterList);
        ResultScanner scanner = HbaseAdministration.table.getScanner(scan);
    }
  
    public void timeQuery3() throws IOException{
        double millisecondi = Math.pow(10, 6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100_records/Query3";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/1000_records/Query3";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/10000_records/Query3";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100000_records/Query3";        
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 3- ---> Hbase");
        /*** Eseguo la query 3 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery3();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query3();
        System.out.println("Fine query 3 ---> Hbase\n");
        w.flush();
        w.close();  
    }
  
        
 // Query 4: Trova la persona di nome "Dominick Condon" che ha effettuato delle transazioni 
 //          con un importo > di 45000 euro sul conto corrente di "Les Rodders"
    public void query4() throws IOException{
        List<Filter> filters = new ArrayList<>();
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_PERSON,
            Bytes.toBytes("nome_cognomeP1"), CompareOp.EQUAL, Bytes.toBytes("Dominick Condon"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT,
            Bytes.toBytes("nome_cognomeP2"), CompareOp.EQUAL, Bytes.toBytes("Les Rodders"));
        filters.add(filter2);
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT,
            Bytes.toBytes("importo"), CompareOp.GREATER, Bytes.toBytes("€45000"));
        filters.add(filter3);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters); 
        scan.setFilter(filterList);
        ResultScanner scanner = HbaseAdministration.table.getScanner(scan);
        //stampo il risultato di scan
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }
    }

    public void esecuzioneQuery4() throws IOException{
        List<Filter> filters = new ArrayList<>();
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_PERSON,
            Bytes.toBytes("nome_cognomeP1"), CompareOp.EQUAL, Bytes.toBytes("Dominick Condon"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT,
            Bytes.toBytes("nome_cognomeP2"), CompareOp.EQUAL, Bytes.toBytes("Les Rodders"));
        filters.add(filter2);
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT,
            Bytes.toBytes("importo"), CompareOp.GREATER, Bytes.toBytes("€45000"));
        filters.add(filter3);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters); 
        scan.setFilter(filterList);
        ResultScanner scanner = HbaseAdministration.table.getScanner(scan);
    }
     
    public void timeQuery4() throws IOException{
        double millisecondi = Math.pow(10, 6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100_records/Query4";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/1000_records/Query4";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/10000_records/Query4";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100000_records/Query4";        
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 4 ---> Hbase");
        /*** Eseguo la query 4 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery4();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query4();
        System.out.println("Fine query 4 ---> Hbase\n");
        w.flush();
        w.close();  
    }
    
 // Query 5: Trova la società che ha effettuato delle transazioni il 15 Gennaio del 2044 sul conto corrente 
 //          posseduto da "Dorotea Menure" che è parente con delle persone che possiedono quote sociali 
 //          maggiori al 25% riguardanti la societa stessa
    public void query5() throws IOException{
        List<Filter> filters = new ArrayList<>();
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT, 
            Bytes.toBytes("data_transazione"), CompareOp.EQUAL, Bytes.toBytes("15/01/2044"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT, 
            Bytes.toBytes("nome_cognomeP2"), CompareOp.EQUAL, Bytes.toBytes("Dorotea Menure"));
        filters.add(filter2);
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_SOCIETY, 
            Bytes.toBytes("quote"), CompareOp.GREATER, Bytes.toBytes("25.0"));
        filters.add(filter3);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters); 
        scan.setFilter(filterList);
        ResultScanner scanner = HbaseAdministration.table.getScanner(scan);
        //stampo il risultato di scan
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }
    }
     
    public void esecuzioneQuery5() throws IOException{
        List<Filter> filters = new ArrayList<>();
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT, 
            Bytes.toBytes("data_transazione"), CompareOp.EQUAL, Bytes.toBytes("15/01/2044"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_BANKACCOUNT, 
            Bytes.toBytes("nome_cognomeP2"), CompareOp.EQUAL, Bytes.toBytes("Dorotea Menure"));
        filters.add(filter2);
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(HbaseAdministration.MY_COLUMN_FAMILY_NAME_SOCIETY, 
            Bytes.toBytes("quote"), CompareOp.GREATER, Bytes.toBytes("25.0"));
        filters.add(filter3);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters); 
        scan.setFilter(filterList);  
        ResultScanner scanner = HbaseAdministration.table.getScanner(scan);
    }

    public void timeQuery5() throws IOException{
        double millisecondi = Math.pow(10, 6);
        FileWriter w;
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100_records/Query5";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/1000_records/Query5";
        //String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/10000_records/Query5";
        String percorso = System.getProperty("user.dir") + File.separator + "/Query_Time/HBase/100000_records/Query5";        
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 5 ---> Hbase");
        /*** Eseguo la query 5 per 31 volte ***/
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            esecuzioneQuery5();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " \n");
        }
        query5();
        System.out.println("Fine query 5 ---> Hbase\n");
        w.flush();
        w.close();  
    }

}