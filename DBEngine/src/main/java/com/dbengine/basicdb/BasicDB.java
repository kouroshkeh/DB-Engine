package com.dbengine.basicdb;

import java.io.*;

public class BasicDB {
    private File dbFile;

    public BasicDB(String dbName){
        this.dbFile = new File(dbName);
        try {
            dbFile.createNewFile();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void db_set(String key, String value){
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(dbFile, true)))) {
            out.println(key + "," + value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String db_get(String key){
        try (RandomAccessFile raf = new RandomAccessFile(dbFile, "r")) {
            long fileLength = raf.length();
            long pointer = fileLength;

            while (pointer > 0) {
                pointer--;
                raf.seek(pointer);
                int readByte = raf.readByte();

                if (readByte == '\n' || pointer == 0) {
                    String line;
                    if (pointer == 0) {
                        line = raf.readLine();
                    } else {
                        line = raf.readLine();
                    }

                    if (line != null) {
                        String[] kv = line.split(",", 2);
                        if (kv[0].equals(key)) {
                            return kv[1];
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
