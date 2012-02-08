/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package lucene.main;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import lucene.properties.Config;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author duchuynh
 */
public class Indexer {
    public static void main(String[] agrs){
        File indexDir = new File(Config.getParameter("index"));
        long start = new Date().getTime();
        Indexer index = new Indexer();
        int error = index.index(indexDir);
        long end = new Date().getTime();
        if(error == 1){
            System.out.println("Time index :" + (end - start) + " milisecond");
        }else {
            System.out.println("Indexing error!");
        }
    }
    
    /**
     * @author Duc Huynh
     * @param indexDir
     * @return
     */
    public int index(File indexDir){
        try {
            //create Lucene
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_34);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            //connection to DB
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "root");
            String sql = Config.getParameter("db.query");
            Statement stmt = connection.createStatement();  
            ResultSet rs = stmt.executeQuery(sql);
            //index data from query
            while (rs.next()) {
                System.out.println("Indexing id " + rs.getString("id") + " : " + rs.getString("name"));
                Document d = new Document();
                d.add(new Field("id", rs.getString("id"), Field.Store.YES, Field.Index.NO));
                d.add(new Field("name", rs.getString("name"), Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field("color", rs.getString("color"),Field.Store.YES, Field.Index.ANALYZED));
                writer.addDocument(d);
             }
            writer.optimize();
            writer.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return 0;
        }
        return 1;
    }
}
