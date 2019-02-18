package com.dfauth.imports;


import com.dfauth.schema.Labels;
import com.dfauth.schema.RelationshipTypes;

import com.uber.h3core.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
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

public class ImportPharmaciesRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportPharmaciesRunnable(String file, GraphDatabaseService db, Log log) {
        this.file = file;
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
            log.error("ImportPharmaciesRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportPharmaciesRunnable Import - IO Exception: " + file);
        }


        HashSet<String> relationships = new HashSet<>();

        Transaction tx = db.beginTx();
        try {
            int count = 0;
            H3Core h3 = H3Core.newInstance();
            
            assert records != null;
            for (CSVRecord record : records) {
                count++;
                Node pharmacy = null;
                Node pharmacyLocation = null;
                hexAddr = null;

                if (!record.get("biz_name").isEmpty()) {
                    pharmacy = db.createNode(Labels.Pharmacy);
                    pharmacy.setProperty("name", record.get("biz_name"));
                    pharmacy.setProperty("pMedcare", record.get("p_medcare"));
                    pharmacy.setProperty("phone", record.get("biz_phone"));
                }
                
                if (!record.get("e_address").isEmpty()) {
                        String locationKey = record.get("e_address") + "_" + record.get("e_city") + "_" + record.get("e_state") + "_" + record.get("e_postal") ;
                        pharmacyLocation = db.findNode(Labels.Location, "LocationKey", locationKey);
                        if (pharmacyLocation ==null && record.get("loc_LAT_centroid").trim().length()>1){
                            pharmacyLocation = db.createNode(Labels.Location);
                            double latValue = Double.parseDouble(record.get("loc_LAT_centroid").trim());
                            double longValue = Double.parseDouble(record.get("loc_LONG_centroid").trim());
                            pharmacyLocation.setProperty("LocationKey",locationKey);
                            pharmacyLocation.setProperty("Address1",record.get("e_address"));
                            pharmacyLocation.setProperty("CityName",record.get("e_city"));
                            pharmacyLocation.setProperty("StateName",record.get("e_state"));
                            pharmacyLocation.setProperty("PostalCode",record.get("e_postal"));
                            pharmacyLocation.setProperty("location",pointValue( WGS84,longValue, latValue));
                            pharmacyLocation.setProperty("latitude", latValue);
                            pharmacyLocation.setProperty("longitude", longValue);
                            hexAddr = h3.geoToH3Address(latValue, longValue, h3Resolution);
                            pharmacyLocation.setProperty("hexAddr",hexAddr);
                       }
                }

                if (pharmacy !=null && pharmacyLocation !=null){
                    {
                        pharmacy.createRelationshipTo(pharmacyLocation, RelationshipTypes.IS_LOCATED_AT);
                    }
                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
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
