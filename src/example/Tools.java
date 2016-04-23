package example;

import java.io.*;
/**
 * Created by edward on 16-4-21.
 */
public class Tools {
    static public void deleteDir(File file) throws FileNotFoundException {
        if(!file.exists())
            throw new FileNotFoundException();

        if(file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files)
                deleteDir(f);
        }

        file.delete();
    }
}
