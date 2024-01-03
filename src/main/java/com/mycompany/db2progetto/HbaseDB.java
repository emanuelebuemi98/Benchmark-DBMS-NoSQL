
package com.mycompany.db2progetto;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public final class HbaseDB {
    public HbaseDB() {
    }
    

    public void importLocalFileToHBase(String fileName, Table table, byte[] columnFamily, String[] column) {
        long st = System.currentTimeMillis();
        try{	
            int count = 0;
            Reader csvData = new FileReader(fileName);
            CSVParser parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(csvData);
            for (CSVRecord csvRecord : parser) {
                String rowKey = csvRecord.get("id");
                Put put = new Put(Bytes.toBytes(rowKey));
           
                for(int i=1;i<column.length;i++){
                    put.addColumn(columnFamily, Bytes.toBytes(column[i]),Bytes.toBytes(csvRecord.get(column[i])));
                }
                try {
                   table.put(put);
                }
                catch (IOException e) {
                }
                
                //stampo il contenuto della table --> ColumnFamily : [column == value, ..]
                Result row = table.get(new Get(Bytes.toBytes(rowKey)));
                
                System.out.println("Riga [" + Bytes.toString(row.getRow())
                   + "] è stata recuperata dalla tabella ["
                   + table.getName().getNameAsString()
                   + "] in HBase, con questo contenuto:");

                for (Entry<byte[], NavigableMap<byte[], byte[]>> colFamilyEntry
                    : row.getNoVersionMap().entrySet()) {
                    String columnFamilyName = Bytes.toString(colFamilyEntry.getKey());

                    System.out.println(" Columns in Column Family [" + columnFamilyName
                        + "]:");

                    for (Entry<byte[], byte[]> columnNameAndValueMap
                        : colFamilyEntry.getValue().entrySet()) {

                    System.out.println("    Value of Column [" + columnFamilyName + ":"
                        + Bytes.toString(columnNameAndValueMap.getKey()) + "] == "
                        + Bytes.toString(columnNameAndValueMap.getValue()));
                    }
                }
            }
        }
        catch (IOException e) {
        } 
        finally { 
            try {
                table.close(); 
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long en2 = System.currentTimeMillis();
        System.out.println("Time: " + (en2 - st) + " ms");
    } 
         

    /**
    *
    * @param admin Standard Admin object
     * @param name_space
     * @param table_name
     * @param columnFamilyPerson
     * @param columnFamilyBankAccount
     * @param columnFamilySociety
    * @throws IOException If IO problem encountered
    */
    public void createNamespaceAndTable(final Admin admin, String name_space, TableName table_name, 
            byte[] columnFamilyPerson, byte[] columnFamilySociety, byte[] columnFamilyBankAccount) throws IOException {
        if (!namespaceExists(admin, name_space)) {
             System.out.println("Creazione del Namespace [" + name_space + "].");

             admin.createNamespace(NamespaceDescriptor
                .create(name_space).build());
        }
        if (!admin.tableExists(table_name)) {
            System.out.println("Creazione della Table [" + table_name.getNameAsString()
                + "], con Column Family ["
                + Bytes.toString(columnFamilyPerson) + ", "
                + Bytes.toString(columnFamilySociety) +  ", "
                + Bytes.toString(columnFamilyBankAccount) + "].");
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(table_name)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamilyPerson))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamilySociety))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamilyBankAccount))
                .build();
            admin.createTable(desc);
        }
    }

    /**
    *
    * @param admin Standard Admin object
    * @param namespaceName Name of namespace
    * @return true If namespace exists
    * @throws IOException If IO problem encountered
    */
    static boolean namespaceExists(final Admin admin, final String namespaceName) throws IOException {
        try {
           admin.getNamespaceDescriptor(namespaceName);
        } catch (NamespaceNotFoundException e) {
          return false;
        }
        return true;
    }

}