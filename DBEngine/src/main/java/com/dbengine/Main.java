package com.dbengine;

import com.dbengine.basicdb.BasicDB;
import com.dbengine.indexdb.IndexDB;

public class Main {
    public static void main(String[] args) {
        BasicDB db = new BasicDB("database.txt");

        db.db_set("123456", "{\"name\":\"London\",\"attractions\":[\"Big Ben\",\"London Eye\"]}");
        db.db_set("42", "{\"name\":\"San Francisco\",\"attractions\":[\"Golden Gate Bridge\"]}");
        db.db_set("42", "{\"name\":\"Tokyo\",\"attractions\":[\"Golden Gate Bridge\"]}");

        System.out.println(db.db_get("42"));

        IndexDB idb = new IndexDB("indexed_database.txt");
        idb.db_set("123456", "{\"name\":\"London\",\"attractions\":[\"Big Ben\",\"London Eye\"]}");
        idb.db_set("42", "{\"name\":\"San Francisco\",\"attractions\":[\"Golden Gate Bridge\"]}");
        idb.db_set("42", "{\"name\":\"Tokyo\",\"attractions\":[\"Golden Gate Bridge\"]}");

        System.out.println(idb.db_get("42"));
        idb.compact();
        System.out.println(idb.db_get("42"));



    }
}
