package com.dbengine.indexdb;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class IndexDB {
    private File dbFile;
    private Map<String, Long> index;

    public IndexDB(String fileName) {
        this.dbFile = new File(fileName);
        this.index = new HashMap<>();
        try {
            if (!dbFile.createNewFile()) {
                rebuildIndex();
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private void rebuildIndex() {
        try (RandomAccessFile raf = new RandomAccessFile(dbFile, "r")) {
            long pointer = 0;
            String line;
            while ((line = raf.readLine()) != null) {
                String[] kv = line.split(",", 2);
                if (kv.length == 2) {
                    index.put(kv[0], pointer);
                }
                pointer = raf.getFilePointer();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void db_set(String key, String value) {
        try (RandomAccessFile raf = new RandomAccessFile(dbFile, "rw")) {
            long pointer = raf.length();
            raf.seek(pointer);
            raf.writeBytes(key + "," + value + "\n");
            index.put(key, pointer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String db_get(String key) {
        Long pointer = index.get(key);
        if (pointer == null) {
            return null;
        }
        try (RandomAccessFile raf = new RandomAccessFile(dbFile, "r")) {
            raf.seek(pointer);
            String line = raf.readLine();
            if (line != null) {
                String[] kv = line.split(",", 2);
                if (kv.length == 2 && kv[0].equals(key)) {
                    return kv[1];
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void compact() {
        File tempFile = new File(dbFile.getAbsolutePath() + ".tmp");
        Map<String, Boolean> processedKeys = new HashMap<>();

        try (RandomAccessFile raf = new RandomAccessFile(dbFile, "r");
             BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {

            long filePointer = raf.length();
            String line;

            while (filePointer > 0) {
                filePointer--;
                raf.seek(filePointer);

                if (filePointer == 0 || raf.readByte() == '\n') {
                    if (filePointer != 0) {
                        line = raf.readLine();
                    } else {
                        raf.seek(0);
                        line = raf.readLine();
                    }

                    if (line != null) {
                        String[] kv = line.split(",", 2);
                        if (kv.length == 2 && !processedKeys.containsKey(kv[0])) {
                            writer.write(line + "\n");
                            processedKeys.put(kv[0], true);
                        }
                    }
                }
            }

            writer.flush();

            if (dbFile.delete()) {
                tempFile.renameTo(dbFile);
                rebuildIndex();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}