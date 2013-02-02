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
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

    /**
     * calculateIndex
     *
     * @return h_index, g_index width publicationList
     * @throws Exception
     */
    public static LinkedHashMap<String, Integer> getCalculateIndex(ArrayList<Integer> publicationList) throws Exception {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<String, Integer>();
        int h_index;
        int g_index;
        int citationCount;
        int citationCountSum;
        // Calculate h-index for each.
        h_index = 0;
        while (h_index < publicationList.size()) {
            citationCount = publicationList.get(h_index);
            if (citationCount >= (h_index + 1)) {
                h_index++;
            } else {
                break;
            }
        }
        // Calculate g-index for each.
        g_index = 0;
        citationCountSum = 0;
        while (true) {
            if (g_index < publicationList.size()) {
                citationCountSum += publicationList.get(g_index);
            }
            if (citationCountSum >= ((g_index + 1) * (g_index + 1))) {
                g_index++;
            } else {
                break;
            }
        }
        out.put("h_index", h_index);
        out.put("g_index", g_index);
        return out;
    }
}