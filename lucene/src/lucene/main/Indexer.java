/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package lucene.main;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import lucene.properties.Config;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.json.simple.JSONObject;

/**
 *
 * @author duchuynh
 */
public class Indexer {
    public static void main(String[] agrs){
        File indexDir = new File(Config.getParameter("index"));
        long start = new Date().getTime();
        Indexer index = new Indexer();
        int count = index.index(indexDir);
        long end = new Date().getTime();
        System.out.println("Index : "+ count +" files : Time index :" + (end - start) + " milisecond");
        
    }
    
    /**
     * @author Duc Huynh
     * @param indexDir
     * @return
     */
    public int index(File indexDir){
        int count;
        try {
            //create Lucene
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_34);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            //connection to DB
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost/cspublicationcrawler", "root", "root");
            String sql = Config.getParameter("db.query");
            PreparedStatement stat = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
            stat.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stat.executeQuery();
            //index data from query            
            count = 0;
            PaperInfo paper = null;
            while (rs.next()) {
                System.out.println("Indexing : " + count++);
                paper = new PaperInfo();
                Document d = new Document();
                paper.setIdPaper(rs.getString(Config.getParameter("field.idPaper")));
                paper.setTitle(rs.getString(Config.getParameter("field.title")));
                paper.setAbstractContent(rs.getString(Config.getParameter("field.abstract")));
                paper.setAuthors(rs.getString(Config.getParameter("field.authorName")));
                paper.setYear(rs.getInt(Config.getParameter("field.year")));
                paper.setConferenceName(rs.getString(Config.getParameter("field.conferenceName")));
                paper.setListAuthor(getListAuthor(Integer.parseInt(paper.idPaper)));                
                d.add(new Field(Config.getParameter("field.idPaper"), paper.idPaper, Field.Store.YES, Field.Index.NO));
                d.add(new Field(Config.getParameter("field.title"), paper.title, Field.Store.YES, Field.Index.ANALYZED));     
                d.add(new Field(Config.getParameter("field.abstract"), paper.abstractContent, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.authorName"), paper.authors, Field.Store.YES, Field.Index.ANALYZED));                
                d.add(new Field(Config.getParameter("field.authorsName"), paper.listAuthor, Field.Store.YES, Field.Index.NO));
                NumericField year = new NumericField(Config.getParameter("field.year"), Field.Store.YES, true);
                year.setIntValue(paper.year);
                d.add(year);
                d.add(new Field(Config.getParameter("field.conferenceName"), paper.conferenceName, Field.Store.YES, Field.Index.ANALYZED));                
                writer.addDocument(d);    
                year = null;
                d = null;
                paper = null;
             }
            count = writer.numDocs();
            writer.optimize();
            writer.close();
            connection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return 0;
        }        
        return count;
    }
    public String getListAuthor(int idPaper) throws SQLException {        
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost/cspublicationcrawler", "root", "root");        
        String sql = Config.getParameter("db.query.list.author");
        PreparedStatement stat = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        stat.setInt(1, idPaper);
        stat.setFetchSize(Integer.MIN_VALUE);        
        ResultSet rs = stat.executeQuery();  
        Map json = new HashMap();        
        ArrayList<Object> list = new ArrayList<Object>();
        while (rs.next()) {
            //db.authorTable.idAuthor
            LinkedHashMap<String, Object> author = new LinkedHashMap<String, Object>();
            author.put("id", rs.getInt(Config.getParameter("db.authorTable.idAuthor")));
            author.put("name", rs.getString(Config.getParameter("db.authorTable.authorName")));
            list.add(author);
        }
        json.put("authors", list);
        JSONObject outJSON = new JSONObject(json);
        connection.close();
        return outJSON.toJSONString();
    }
}
