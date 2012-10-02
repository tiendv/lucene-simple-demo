/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package constant;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.lucene.store.FSDirectory;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 *
 * @author DucHuynh, HuyDang
 */
public class Common {

    public static FSDirectory getFSDirectory(String path, String folder) {
        FSDirectory directory = null;
        try {
            if (path != null && folder != null) {
                File location = new File(path + folder);
                directory = FSDirectory.open(location);
                System.out.println("FSDirectory : " + directory.getDirectory().getPath());
            }
            return directory;
        } catch (Exception e) {
            //write log
            System.out.println(e.getMessage());
            return null;
        }
    }

    /**
     * Convert Base64 string from the object.
     */
    public static String OToS(Object obj) {
        //long start = System.currentTimeMillis();
        String out = null;
        if (obj != null) {
            try {
                BASE64Encoder encode = new BASE64Encoder();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(obj);
                out = encode.encode(baos.toByteArray());
                baos.close();
                oos.close();
            } catch (IOException ex) {
                ex.printStackTrace();
                return null;
            }
        }
        //long end = System.currentTimeMillis();
        //System.out.println("Encode:" + (end - start));
        return out;
    }

    /**
     * Read the object from Base64 string.
     */
    public static Object SToO(String str) {
        //long start = System.currentTimeMillis();
        Object out = null;
        if (str != null) {
            try {
                BASE64Decoder decode = new BASE64Decoder();
                ByteArrayInputStream bios = new ByteArrayInputStream(decode.decodeBuffer(str));
                ObjectInputStream ois = new ObjectInputStream(bios);
                out = ois.readObject();
                bios.close();
                ois.close();
            } catch (IOException ex) {
                ex.printStackTrace();
                return null;
            } catch (ClassNotFoundException ex) {
                ex.printStackTrace();
                return null;
            }
        }
        //long end = System.currentTimeMillis();
        //System.out.println("Decode:" + (end - start));
        return out;
    }
}