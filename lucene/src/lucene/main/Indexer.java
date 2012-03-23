/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package lucene.main;


import java.io.File;
import java.sql.Connection;
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
import lucene.main.ConnectionPool;



/**
 *
 * @author duchuynh
 */
public class Indexer {
    private ConnectionPool connectionPool ;
    public void runIndex(String username, String password, String database, String path){
                
        File indexDir = new File(path);
        connectionPool   = new ConnectionPool(username,password,database);
        long start = new Date().getTime();
        Indexer index = new Indexer();
        int count = index.index(indexDir,connectionPool);
        long end = new Date().getTime();
        System.out.println("Index : "+ count +" files : Time index :" + (end - start) + " milisecond");
    }
    
    /**
     * @author Duc Huynh
     * @param indexDir
     * @return
     */
    public int index(File indexDir, ConnectionPool connectionPool){
        int count;
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_34);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            //connection to DB
           
            Connection connection = connectionPool.getConnection();
            
            String sql = Config.getParameter("db.query");
            PreparedStatement stat = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
            
            stat.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stat.executeQuery();
            //index data from query            
            count = 0;
            PaperInfo paper = null;
            while (rs.next()) {                
                paper = new PaperInfo();
                Document d = new Document();
                paper.setIdPaper(rs.getString(Config.getParameter("field.idPaper")));
                paper.setTitle(rs.getString(Config.getParameter("field.title")));
                paper.setAbstractContent(rs.getString(Config.getParameter("field.abstract")));
                paper.setYear(rs.getInt(Config.getParameter("field.year")));
                paper.setConferenceName(this.getConferenceName(Integer.parseInt(paper.idPaper)));
                paper.setJournalName(this.getJournalName(Integer.parseInt(paper.idPaper)));
                LinkedHashMap<String, String> listAuthors = getListAuthor(Integer.parseInt(paper.idPaper));
                paper.setListAuthor(listAuthors.get("json"));       
                paper.setAuthors(listAuthors.get("token"));
                d.add(new Field(Config.getParameter("field.idPaper"), paper.idPaper, Field.Store.YES, Field.Index.NO));
                d.add(new Field(Config.getParameter("field.title"), paper.title, Field.Store.YES, Field.Index.ANALYZED));     
                d.add(new Field(Config.getParameter("field.abstract"), paper.abstractContent, Field.Store.YES, Field.Index.ANALYZED)); 
                d.add(new Field(Config.getParameter("field.authorName"), paper.authors, Field.Store.YES, Field.Index.ANALYZED)); 
                d.add(new Field(Config.getParameter("field.authorsName"), paper.listAuthor, Field.Store.YES, Field.Index.NO));
                NumericField year = new NumericField(Config.getParameter("field.year"), Field.Store.YES, true);
                year.setIntValue(paper.year);
                d.add(year);
                d.add(new Field(Config.getParameter("field.conferenceName"), paper.conferenceName, Field.Store.YES, Field.Index.ANALYZED));                                    
                System.out.println("Indexing : " + count++ + "\t" + paper.title);
                writer.addDocument(d);
                year = null;
                d = null;
                paper = null;
             }
            count = writer.numDocs();
            writer.optimize();
            writer.close();
            stat.close();
            connection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return 0;
        }        
        return count;
    }
    public LinkedHashMap<String, String> getListAuthor(int idPaper) throws SQLException, ClassNotFoundException { 
        LinkedHashMap<String, String> ret = new LinkedHashMap<String, String>();
        Connection connection = ConnectionPool.dataSource.getConnection();
        String sql = Config.getParameter("db.query.list.author");
        PreparedStatement stat = connection.prepareStatement(sql);
        stat.setInt(1, idPaper);
        stat.setFetchSize(Integer.MIN_VALUE);        
        ResultSet rs = stat.executeQuery();  
        Map json = new HashMap();        
        ArrayList<Object> list = new ArrayList<Object>();
        StringBuilder str = new StringBuilder();
        while (rs.next()) {
            //db.authorTable.idAuthor
            LinkedHashMap<String, Object> author = new LinkedHashMap<String, Object>();
            author.put("id", rs.getInt(Config.getParameter("db.authorTable.idAuthor")));            
            String name = rs.getString(Config.getParameter("db.authorTable.authorName"));
            author.put("name", name);
            str.append(name);
            str.append(" ");            
            list.add(author);
        }
        json.put("authors", list);
        JSONObject outJSON = new JSONObject(json);
        ret.put("json", outJSON.toJSONString());
        ret.put("token", str.toString());
        stat.close();
        connection.close();        
        return ret;
    }
    public String getConferenceName(int idPaper) throws SQLException, ClassNotFoundException {
        StringBuilder sql = new StringBuilder();
        sql.append(Config.getParameter("db.query.conference.name"));
        Connection con = ConnectionPool.dataSource.getConnection();
        if (con != null) {
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            stmt.setInt(1, idPaper);
            ResultSet rs = stmt.executeQuery();
            if ((rs != null) && (rs.next())) {
                String name = rs.getString(Config.getParameter("db.conference.table.name"));
                stmt.close();
                con.close();
                return name;
            } else {
                stmt.close();
                con.close();
                return null;
            }
        } else {
            return null;
        }
    }
    public String getJournalName(int idPaper) throws SQLException, ClassNotFoundException {
        StringBuilder sql = new StringBuilder();
        sql.append(Config.getParameter("db.query.journal.name"));
        Connection con = ConnectionPool.dataSource.getConnection();
        if (con != null) {
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            stmt.setInt(1, idPaper);
            ResultSet rs = stmt.executeQuery();            
            if ((rs != null) && (rs.next())) {
                String name = rs.getString(Config.getParameter("db.journal.table.name"));
                stmt.close();
                con.close();
                return name;
            } else {
                stmt.close();
                con.close();
                return null;
            }
        } else {
            return null;
        }
    }
}