/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.ConferenceTB;
import database.JournalTB;
import database.KeywordTB;
import database.OrgTB;
import database.PaperKeywordTB;
import database.PaperPaperTB;
import database.PaperTB;
import database.RankPaperTB;
import database.SubdomainPaperTB;
import database.SubdomainTB;
import dto.PaperDTO;
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
 * @author HuyDang
 */
public class PaperIndexer {

    private ConnectionPool connectionPool;

    public String _run(String username, String password, String database, int port) {
        String out = "";
        try {
            File indexDir = new File(IndexConst.PAPER_INDEX_PATH);
            connectionPool = new ConnectionPool(username, password, database, port);
            long start = new Date().getTime();
            PaperIndexer paperIndexer = new PaperIndexer();
            int count = paperIndexer._index(indexDir, connectionPool);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    public int _index(File indexDir, ConnectionPool connectionPool) {
        int count = 0;
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            // Connection to DB           
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT * FROM " + PaperTB.TABLE_NAME + " p";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            PaperDTO paper = null;
            while ((rs != null) && (rs.next())) {
                paper = new PaperDTO();
                Document d = new Document();
                // ListAuthor
                LinkedHashMap<String, String> listAuthors = this.getListAuthor(rs.getInt(PaperTB.COLUMN_PAPERID));
                LinkedHashMap<String, String> listCitations = this.getListCitations(rs.getInt(PaperTB.COLUMN_PAPERID));
                LinkedHashMap<String, String> listKeywords = this.getListKeywords(rs.getInt(PaperTB.COLUMN_PAPERID));
                paper.setIdPaper(rs.getString(PaperTB.COLUMN_PAPERID));
                paper.setTitle(rs.getString(PaperTB.COLUMN_TITLE));
                paper.setAbstractContent(rs.getString(PaperTB.COLUMN_ABSTRACT));
                paper.setDoi(rs.getString(PaperTB.COLUMN_DOI));
                paper.setIdConference(rs.getString(PaperTB.COLUMN_CONFERENCEID));
                paper.setIdJournal(rs.getString(PaperTB.COLUMN_JOURNALID));
                paper.setIsbn(rs.getString(PaperTB.COLUMN_ISBN));
                paper.setPages(rs.getString(PaperTB.COLUMN_PAGES));
                paper.setViewPublication(rs.getString(PaperTB.COLUMN_VIEWPUBLICATION));
                paper.setVolume(rs.getString(PaperTB.COLUMN_VOLUME));
                paper.setYear(rs.getInt(PaperTB.COLUMN_YEAR));
                paper.setConferenceName(this.getConferenceName(rs.getInt(PaperTB.COLUMN_PAPERID)));
                paper.setJournalName(this.getJournalName(rs.getInt(PaperTB.COLUMN_PAPERID)));
                paper.setListIdSubdomains(this.getListIdSubdomains(rs.getInt(PaperTB.COLUMN_PAPERID)));
                paper.setKeywordsName(listKeywords.get("keywords"));
                paper.setOrgsName(listAuthors.get("orgsName"));
                paper.setAuthorsName(listAuthors.get("authorsName"));
                paper.setAuthors(listAuthors.get("authors"));
                paper.setCitationCount(Integer.parseInt(listCitations.get("citationCount")));
                paper.setListCitations(listCitations.get("listCitations"));
                paper.setListIdAuthors(listAuthors.get("listIdAuthors"));
                paper.setListIdKeywords(listKeywords.get("listIdKeywords"));
                paper.setListIdOrgs(listAuthors.get("listIdOrgs"));
                paper.setRank(this.getRank(rs.getInt(PaperTB.COLUMN_PAPERID)));

                d.add(new Field(IndexConst.PAPER_IDPAPER_FIELD, paper.idPaper, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_TITLE_FIELD, paper.title, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_ABSTRACT_FIELD, paper.abstractContent, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_CONFERENCENAME_FIELD, paper.conferenceName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_DOI_FIELD, paper.doi, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_ORGSNAME_FIELD, paper.orgsName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_IDCONFERENCE_FIELD, paper.idConference, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_IDJOURNAL_FIELD, paper.idJournal, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_ISBN_FIELD, paper.isbn, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_JOURNALNAME_FIELD, paper.journalName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_PAGES_FIELD, paper.pages, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_VOLUME_FIELD, paper.volume, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_VIEWPUBLICATION_FIELD, paper.viewPublication, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_AUTHORS_FIELD, paper.authors, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.PAPER_AUTHORSNAME_FIELD, paper.authorsName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_KEYWORDSNAME_FIELD, paper.keywordsName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTCITATIONS_FIELD, paper.listCitations, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.PAPER_LISTIDAUTHORS_FIELD, paper.listIdAuthors, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTIDKEYWORDS_FIELD, paper.listIdKeywords, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTIDORGS_FIELD, paper.listIdOrgs, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTIDSUBDOMAINS_FIELD, paper.listIdSubdomains, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new NumericField(IndexConst.PAPER_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(paper.citationCount));
                d.add(new NumericField(IndexConst.PAPER_YEAR_FIELD, Field.Store.YES, true).setIntValue(paper.year));
                d.add(new NumericField(IndexConst.PAPER_RANK_FIELD, Field.Store.YES, true).setIntValue(paper.rank));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + paper.title);
                d = null;
                paper = null;
            }
            count = writer.numDocs();
            writer.optimize();
            writer.close();
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return 0;
        }
        return count;
    }

    public String getConferenceName(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        String name = "";
        String sql = "SELECT c." + ConferenceTB.COLUMN_CONFERENCENAME + " FROM " + PaperTB.TABLE_NAME + " p JOIN " + ConferenceTB.TABLE_NAME + " c ON c." + ConferenceTB.COLUMN_CONFERENCEID + " = p." + PaperTB.COLUMN_CONFERENCEID + " WHERE p." + PaperTB.COLUMN_PAPERID + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        if ((rs != null) && (rs.next())) {
            name = rs.getString(ConferenceTB.COLUMN_CONFERENCENAME);
        }
        stmt.close();
        connection.close();
        return name;
    }

    public String getJournalName(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        String name = "";
        String sql = "SELECT j." + JournalTB.COLUMN_JOURNALNAME + " FROM " + PaperTB.TABLE_NAME + " p JOIN " + JournalTB.TABLE_NAME + " j ON j." + JournalTB.COLUMN_JOURNALID + " = p." + PaperTB.COLUMN_JOURNALID + " WHERE p." + PaperTB.COLUMN_PAPERID + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        if ((rs != null) && (rs.next())) {
            name = rs.getString(JournalTB.COLUMN_JOURNALNAME);
        }
        stmt.close();
        connection.close();
        return name;
    }

    /* 
     * getListAuthor
     * @param idPaper
     * @return authors {idAuthor, authorName}, authorsName, listIdAuthors, listIdOrgs, orgsName
     */
    public LinkedHashMap<String, String> getListAuthor(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
        Map json = new HashMap();
        ArrayList<Object> authors = new ArrayList<Object>();
        String authorsName = "";
        String listIdAuthors = "";
        String listIdOrgs = "";
        String orgsName = "";
        String sql = "SELECT a." + AuthorTB.COLUMN_AUTHORID + ", a." + AuthorTB.COLUMN_ORGID + ", a." + AuthorTB.COLUMN_AUTHORNAME + ",(SELECT o." + OrgTB.COLUMN_ORGNAME + " FROM " + OrgTB.TABLE_NAME + " o WHERE o." + OrgTB.COLUMN_ORGID + " = a." + AuthorTB.COLUMN_ORGID + ") AS " + OrgTB.COLUMN_ORGNAME + " FROM " + PaperTB.TABLE_NAME + " p JOIN " + AuthorPaperTB.TABLE_NAME + " ap ON ap." + AuthorPaperTB.COLUMN_PAPERID + " = p." + PaperTB.COLUMN_PAPERID + " JOIN " + AuthorTB.TABLE_NAME + " a ON a." + AuthorTB.COLUMN_AUTHORID + " = ap." + AuthorPaperTB.COLUMN_AUTHORID + " WHERE p." + PaperTB.COLUMN_PAPERID + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            //db.authorTB.idAuthor
            LinkedHashMap<String, Object> author = new LinkedHashMap<String, Object>();
            author.put("idAuthor", rs.getInt(AuthorTB.COLUMN_AUTHORID));
            String authorName = rs.getString(AuthorTB.COLUMN_AUTHORNAME);
            author.put("authorName", authorName);
            authorsName += " " + authorName;
            orgsName += " " + rs.getString(OrgTB.COLUMN_ORGNAME);;
            listIdAuthors += " " + rs.getString(AuthorTB.COLUMN_AUTHORID);
            if ((rs.getInt(AuthorTB.COLUMN_ORGID) > 0) && (!listIdOrgs.contains(rs.getString(AuthorTB.COLUMN_ORGID)))) {
                listIdOrgs += " " + rs.getString(AuthorTB.COLUMN_ORGID);
            }
            authors.add(author);
        }
        if (!"".equals(authorsName)) {
            authorsName = authorsName.substring(1);
        }
        if (!"".equals(listIdAuthors)) {
            listIdAuthors = listIdAuthors.substring(1);
        }
        if (!"".equals(listIdOrgs)) {
            listIdOrgs = listIdOrgs.substring(1);
        }
        if (!"".equals(orgsName)) {
            orgsName = orgsName.substring(1);
        }
        json.put("authors", authors);
        JSONObject outJSON = new JSONObject(json);
        result.put("authors", outJSON.toJSONString());
        result.put("authorsName", authorsName);
        result.put("listIdAuthors", listIdAuthors);
        result.put("listIdOrgs", listIdOrgs);
        result.put("orgsName", orgsName);
        stmt.close();
        connection.close();
        return result;
    }

    /*
     * getListCitations
     * @param idPaper
     * @return citationCount, listCitations {citation, year}
     */
    public LinkedHashMap<String, String> getListCitations(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
        ArrayList<Object> listCitations = new ArrayList<Object>();
        int citationCount = 0;
        String sql = "SELECT COUNT(pp." + PaperPaperTB.COLUMN_PAPERID + ") AS citation, (SELECT p." + PaperTB.COLUMN_YEAR + " FROM " + PaperTB.TABLE_NAME + " p WHERE p." + PaperTB.COLUMN_PAPERID + " = pp." + PaperPaperTB.COLUMN_PAPERID + ") AS `year` FROM " + PaperPaperTB.TABLE_NAME + " pp WHERE pp." + PaperPaperTB.COLUMN_PAPERREFID + " = ? GROUP BY `year` ORDER BY `year` ASC";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            citationCount += rs.getInt("citation");
            if (rs.getInt("year") > 0) {
                LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
                temp.put("citation", rs.getInt("citation"));
                temp.put("year", rs.getInt("year"));
                listCitations.add(temp);
            }
        }
        result.put("listCitations", Common.OToS(listCitations));
        result.put("citationCount", Integer.toString(citationCount));
        stmt.close();
        connection.close();
        return result;
    }

    public String getListIdSubdomains(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        String list = "";
        String sql = "SELECT s." + SubdomainTB.COLUMN_SUBDOMAINID + " FROM " + PaperTB.TABLE_NAME + " p JOIN " + SubdomainPaperTB.TABLE_NAME + " sp ON sp." + SubdomainPaperTB.COLUMN_PAPERID + " = p." + PaperTB.COLUMN_PAPERID + " JOIN " + SubdomainTB.TABLE_NAME + " s ON s." + SubdomainTB.COLUMN_SUBDOMAINID + " = sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + " WHERE p." + PaperTB.COLUMN_PAPERID + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            list += " " + rs.getString(SubdomainTB.COLUMN_SUBDOMAINID);
        }
        if (!"".equals(list)) {
            list = list.substring(1);
        }
        stmt.close();
        connection.close();
        return list;
    }

    public LinkedHashMap<String, String> getListKeywords(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
        String listIdKeywords = "";
        String keywords = "";
        String sql = "SELECT pk." + PaperKeywordTB.COLUMN_KEYWORDID + ", (SELECT k." + KeywordTB.COLUMN_KEYWORD + " FROM " + KeywordTB.TABLE_NAME + " k WHERE pk." + PaperKeywordTB.COLUMN_KEYWORDID + " = k." + KeywordTB.COLUMN_KEYWORDID + ") AS " + KeywordTB.COLUMN_KEYWORD + " FROM " + PaperKeywordTB.TABLE_NAME + " pk WHERE pk." + PaperKeywordTB.COLUMN_PAPERID + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            listIdKeywords += " " + rs.getString(PaperKeywordTB.COLUMN_KEYWORDID);
            keywords += " " + rs.getString(KeywordTB.COLUMN_KEYWORD);
        }
        if (!"".equals(listIdKeywords)) {
            listIdKeywords = listIdKeywords.substring(1);
        }
        if (!"".equals(keywords)) {
            keywords = keywords.substring(1);
        }
        result.put("listIdKeywords", listIdKeywords);
        result.put("keywords", keywords);
        stmt.close();
        connection.close();
        return result;
    }

    public int getRank(int idPaper) throws SQLException, ClassNotFoundException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        int rank = 0;
        String sql = "SELECT r." + RankPaperTB.COLUMN_RANK + " FROM " + RankPaperTB.TABLE_NAME + " r WHERE r." + RankPaperTB.COLUMN_PAPERID + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idPaper);
        ResultSet rs = stmt.executeQuery();
        if ((rs != null) && (rs.next())) {
            rank = rs.getInt(RankPaperTB.COLUMN_RANK);
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
            PaperIndexer paperIndexer = new PaperIndexer();
            paperIndexer._run(user, pass, database, port);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}
