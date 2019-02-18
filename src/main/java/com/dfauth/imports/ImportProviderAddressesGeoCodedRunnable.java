package com.dfauth.imports;
import com.dfauth.schema.Labels;
import com.dfauth.schema.RelationshipTypes;

import com.uber.h3core.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.Values.pointValue;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;

public class ImportProviderAddressesGeoCodedRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private String addressType;
    private GraphDatabaseService db;
    private Log log;

    public ImportProviderAddressesGeoCodedRunnable(String file, String addressType, GraphDatabaseService db, Log log) {
        this.file = file;
        this.addressType = addressType;
        this.db = db;
        this.log = log;
    }

    @Override
    public void run() {
        Reader in;
        Iterable<CSVRecord> records = null;
        int h3Resolution = 9;
        String hexAddr = null;
        
        try {
            in = new FileReader("/" + file);
            records = CSVFormat.EXCEL.withHeader().parse(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("ImportProviderAddressesGeoCodedRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportProviderAddressesGeoCodedRunnable Import - IO Exception: " + file);
        }


        HashSet<String> relationships = new HashSet<>();

        Transaction tx = db.beginTx();
        try {
            int count = 0;

            assert records != null;
            H3Core h3 = H3Core.newInstance();
            
            for (CSVRecord record : records) {
                count++;
                    Node provider = null;
                    hexAddr = null;
                    if(!record.get(0).isEmpty()){
                        provider = db.findNode(Labels.Provider, "NPI", record.get(0));
                        if (provider !=null) {
                            double latValue = Double.parseDouble(record.get(1).trim());
                            double longValue = Double.parseDouble(record.get(2).trim());
                            if (addressType.equalsIgnoreCase("Billing")){
                                for (Relationship r : provider.getRelationships(RelationshipTypes.HAS_BILLING_ADDRESS_AT, Direction.OUTGOING)) {
                                //if (!r.getEndNode.propertyExists("latitude")){
                                    r.getEndNode().setProperty("location",pointValue( WGS84, longValue, latValue ));
                                    r.getEndNode().setProperty("latitude",latValue);
                                    r.getEndNode().setProperty("longitude",longValue);
                                    hexAddr = h3.geoToH3Address(latValue, longValue, h3Resolution);
                                    r.getEndNode().setProperty("hexAddr",hexAddr);
                                //}
                                }
                            } else if (addressType.equalsIgnoreCase("Practice")){
                                for (Relationship r : provider.getRelationships(RelationshipTypes.HAS_PRACTICE_AT, Direction.OUTGOING)) {
                                    //if (!r.getEndNode.propertyExists("latitude")){
                                        r.getEndNode().setProperty("location",pointValue( WGS84, longValue, latValue ));
                                        r.getEndNode().setProperty("latitude",latValue);
                                        r.getEndNode().setProperty("longitude",longValue);
                                        hexAddr = h3.geoToH3Address(latValue, longValue, h3Resolution);
                                        r.getEndNode().setProperty("hexAddr",hexAddr);
                                    //}
                                    }
                            } 
                        }   
                    }
                
                

                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        }   catch (Exception e) {
            System.out.println(e);
        }  finally {
            tx.close();
        }

    }    
}
