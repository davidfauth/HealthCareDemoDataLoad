package com.dfauth.imports;

import com.dfauth.schema.Labels;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;

public class ImportIP4Runnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportIP4Runnable(String file, GraphDatabaseService db, Log log) {
        this.file = file;
        this.db = db;
        this.log = log;
    }

    @Override
    public void run() {
        Reader in;
        Iterable<CSVRecord> records = null;
        try {
            in = new FileReader("/" + file);
            records = CSVFormat.EXCEL.withHeader().parse(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("ImportIP4Runnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportIP4Runnable Import - IO Exception: " + file);
        }

        HashSet<String> done = new HashSet<>();

        Transaction tx = db.beginTx();
        try {
            int count = 0;

            assert records != null;
            for (CSVRecord record : records) {
                count++;
                String geonameId = record.get("geoname_id");
                if (!done.contains(geonameId)) {
                    Node city = db.findNode(Labels.City, "geoname_id", geonameId);
                    if (city != null) {
                        city.setProperty("latitude", Float.valueOf(record.get("latitude")));
                        city.setProperty("longitude", Float.valueOf(record.get("longitude")));
                        done.add(geonameId);
                    }
                }
            }
            if (count % TRANSACTION_LIMIT == 0) {
                tx.success();
                tx.close();
                tx = db.beginTx();
            }

        tx.success();
    } finally {
        tx.close();
    }

}
}
