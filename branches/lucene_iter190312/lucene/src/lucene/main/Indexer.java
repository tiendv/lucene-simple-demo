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

/**
 *
 * @author DucHuynh, HuyDang
 */
public class Indexer {

    private ConnectionPool connectionPool;

    public void runIndex(String username, String password, String database, int port) {

        File indexDir = new File(Config.getParameter("index"));
        connectionPool = new ConnectionPool(username, password, database, port);

        long start = new Date().getTime();
        Indexer index = new Indexer();
        int count = index.index(indexDir, connectionPool);
        long end = new Date().getTime();
        System.out.println("Index : " + count + " files : Time index :" + (end - start) + " milisecond");
    }

    /**
     * @author Duc Huynh
     * @param indexDir
     * @return
     */
    public int index(File indexDir, ConnectionPool connectionPool) {
        int count;
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_34);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            //connection to DB           
            Connection connection = connectionPool.getConnection();
            String sql = Config.getParameter("db.query");
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
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
                LinkedHashMap<String, String> listAuthors = this.getListAuthor(Integer.parseInt(paper.idPaper));
                paper.setListAuthor(listAuthors.get("json"));
                paper.setAuthors(listAuthors.get("token"));
                paper.idConference = Integer.toString(rs.getInt(Config.getParameter("field.idConference")));
                paper.idJournal = Integer.toString(rs.getInt(Config.getParameter("field.idJournal")));

                String listIdAuthors = listAuthors.get(Config.getParameter("field.listIdAuthors"));
                String listIdOrgs = listAuthors.get(Config.getParameter("field.listIdOrgs"));
                String listCitations = this.getListCitations(Integer.parseInt(paper.idPaper));
                String listIdSubdomains = this.getListIdSubdomains(Integer.parseInt(paper.idPaper));
                String listIdKeywords = this.getListIdKeywords(Integer.parseInt(paper.idPaper));
                LinkedHashMap<String, Integer> rankPaper = this.getRankPaper(Integer.parseInt(paper.idPaper));
                int citationCount = rankPaper.get(Config.getParameter("field.citationCount"));
                int rank = rankPaper.get(Config.getParameter("field.rank"));

                d.add(new Field(Config.getParameter("field.idPaper"), paper.idPaper, Field.Store.YES, Field.Index.NO));
                d.add(new Field(Config.getParameter("field.title"), paper.title, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.abstract"), paper.abstractContent, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.authorName"), paper.authors, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.authorsName"), paper.listAuthor, Field.Store.YES, Field.Index.NO));
                d.add(new Field(Config.getParameter("field.idConference"), paper.idConference, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.conferenceName"), paper.conferenceName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.idJournal"), paper.idJournal, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.journalName"), paper.journalName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.listIdAuthors"), listIdAuthors, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.listIdOrgs"), listIdOrgs, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.listCitations"), listCitations, Field.Store.YES, Field.Index.NO));
                d.add(new Field(Config.getParameter("field.listIdSubdomains"), listIdSubdomains, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(Config.getParameter("field.listIdKeywords"), listIdKeywords, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new NumericField(Config.getParameter("field.citationCount"), Field.Store.YES, true).setIntValue(citationCount));
                d.add(new NumericField(Config.getParameter("field.rank"), Field.Store.YES, true).setIntValue(rank));
                d.add(new NumericField(Config.getParameter("field.year"), Field.Store.YES, true).setIntValue(paper.year));

                System.out.println("Indexing : " + count++ + "\t" + paper.title);
                writer.addDocument(d);
                d = null;
                paper = null;
            }
            count = writer.numDocs();
            writer.optimize();
            writer.close();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return 0;
        }
        return count;
    }

    public LinkedHashMap<String, String> getListAuthor(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        LinkedHashMap<String, String> ret = new LinkedHashMap<String, String>();
        Map json = new HashMap();
        ArrayList<Object> list = new ArrayList<Object>();
        StringBuilder str = new StringBuilder();
        String listIdAuthors = "";
        String listIdOrgs = "";
        String sql = Config.getParameter("db.query.list.author");
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            //db.authorTable.idAuthor
            LinkedHashMap<String, Object> author = new LinkedHashMap<String, Object>();
            author.put("id", rs.getInt(Config.getParameter("db.authorTable.idAuthor")));
            String name = rs.getString(Config.getParameter("db.authorTable.authorName"));
            author.put("name", name);
            str.append(name);
            str.append(" ");
            listIdAuthors += " " + rs.getString(Config.getParameter("db.authorTable.idAuthor"));
            if ((rs.getInt(Config.getParameter("db.authorTable.idOrg")) > 0) && (!listIdOrgs.contains(rs.getString(Config.getParameter("db.authorTable.idOrg"))))) {
                listIdOrgs += " " + rs.getString(Config.getParameter("db.authorTable.idOrg"));
            }
            list.add(author);
        }
        if (!"".equals(listIdAuthors)) {
            listIdAuthors = listIdAuthors.substring(1);
        }
        if (!"".equals(listIdOrgs)) {
            listIdOrgs = listIdOrgs.substring(1);
        }
        json.put("authors", list);
        JSONObject outJSON = new JSONObject(json);
        ret.put("json", outJSON.toJSONString());
        ret.put("token", str.toString());
        ret.put(Config.getParameter("field.listIdAuthors"), listIdAuthors);
        ret.put(Config.getParameter("field.listIdOrgs"), listIdOrgs);
        stmt.close();
        connection.close();
        return ret;
    }

    public String getConferenceName(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        String name = "";
        String sql = Config.getParameter("db.query.conference.name");
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        if ((rs != null) && (rs.next())) {
            name = rs.getString(Config.getParameter("db.conference.table.name"));

        }
        stmt.close();
        connection.close();
        return name;
    }

    public String getJournalName(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        String name = "";
        String sql = Config.getParameter("db.query.journal.name");
        PreparedStatement stmt = connection.prepareStatement(sql.toString());
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        if ((rs != null) && (rs.next())) {
            name = rs.getString(Config.getParameter("db.journal.table.name"));

        }
        stmt.close();
        connection.close();
        return name;
    }

    public String getListCitations(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        ArrayList<Object> list = new ArrayList<Object>();
        String sql = Config.getParameter("db.query.list.citation");
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            if (rs.getInt("year") > 0) {
                LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
                temp.put("count", rs.getInt("count"));
                temp.put("year", rs.getInt("year"));
                list.add(temp);
            }
        }
        stmt.close();
        connection.close();
        return Common.OToS(list);
    }

    public String getListIdSubdomains(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        String list = "";
        String sql = Config.getParameter("db.query.list.subdomain");
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            list += " " + rs.getString(Config.getParameter("db.subdomain.table.idSubdomain"));
        }
        if (!"".equals(list)) {
            list = list.substring(1);
        }
        stmt.close();
        connection.close();
        return list;
    }

    public String getListIdKeywords(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        String list = "";
        String sql = Config.getParameter("db.query.list.keyword");
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            list += " " + rs.getString(Config.getParameter("db.keyword.table.idKeyword"));
        }
        if (!"".equals(list)) {
            list = list.substring(1);
        }
        stmt.close();
        connection.close();
        return list;
    }

    public LinkedHashMap<String, Integer> getRankPaper(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        LinkedHashMap<String, Integer> rank = new LinkedHashMap<String, Integer>();
        String sql = Config.getParameter("db.query.rank.paper");
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        if ((rs != null) && (rs.next())) {
            rank.put(Config.getParameter("field.citationCount"), rs.getInt(Config.getParameter("db.rank.citationCount")));
            rank.put(Config.getParameter("field.rank"), rs.getInt(Config.getParameter("db.rank.rank")));
        } else {
            rank.put(Config.getParameter("field.citationCount"), 0);
            rank.put(Config.getParameter("field.rank"), 0);
        }
        stmt.close();
        connection.close();
        return rank;
    }

    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String user = "root";
            String pass = "@huydang1920@";
            String database = "cspublicationcrawler";
            int port = 3306;

            Indexer indexer = new Indexer();
            indexer.runIndex(user, pass, database, port);

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}