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
import java.io.IOException;
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

    private String path = "E:\\";

    /**
     * hàm khởi tạo
     *
     * @param path
     */
    public PaperIndexer(String path) {
        try {
            this.path = path;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
     * hàm khởi chạy index
     *
     * @param connectionPool
     * @return số doc thực hiện index và thời gian index
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.PAPER_INDEX_PATH);
            long start = new Date().getTime();
            int count = this._index(connectionPool, indexDir);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    /**
     * thực hiện truy vấn các thông tin của paper từ csdl và thực gọi các hàm
     * tính toán các thông tin khác, thực hiện index
     *
     * @param connectionPool
     * @param indexDir
     * @return số doc thực hiện index
     */
    private int _index(ConnectionPool connectionPool, File indexDir) throws IOException {
        int count = 0;

        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(indexDir);
        IndexWriter writer = new IndexWriter(directory, config);
        // Connection to DB           
        Connection connection = connectionPool.getConnection();
        try {
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
                LinkedHashMap<String, String> listAuthor = this.getListAuthor(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID));
                LinkedHashMap<String, String> listCitation = this.getListCitation(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID));
                LinkedHashMap<String, String> listKeyword = this.getListKeyword(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID));
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
                paper.setConferenceName(this.getConferenceName(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID)));
                paper.setJournalName(this.getJournalName(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID)));
                paper.setListIdSubdomain(this.getListIdSubdomains(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID)));
                paper.setKeywordsName(listKeyword.get("keywords"));
                paper.setOrgsName(listAuthor.get("orgsName"));
                paper.setAuthorsName(listAuthor.get("authorsName"));
                paper.setAuthors(listAuthor.get("authors"));
                paper.setCitationCount(Integer.parseInt(listCitation.get("citationCount")));
                paper.setReferenceCount(this.getReferenceCount(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID)));
                paper.setListCitation(listCitation.get("listCitation"));
                paper.setListIdAuthor(listAuthor.get("listIdAuthor"));
                paper.setListIdKeyword(listKeyword.get("listIdKeyword"));
                paper.setListIdOrg(listAuthor.get("listIdOrg"));
                paper.setListIdPaperCitation(this.getListIdPaperCitations(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID)));
                paper.setRank(this.getRank(connectionPool, rs.getInt(PaperTB.COLUMN_PAPERID)));

                d.add(new Field(IndexConst.PAPER_IDPAPER_FIELD, paper.idPaper, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_TITLE_FIELD, paper.title, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                d.add(new Field(IndexConst.PAPER_ABSTRACT_FIELD, paper.abstractContent, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                d.add(new Field(IndexConst.PAPER_CONFERENCENAME_FIELD, paper.conferenceName, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                d.add(new Field(IndexConst.PAPER_DOI_FIELD, paper.doi, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.PAPER_ORGSNAME_FIELD, paper.orgsName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_IDCONFERENCE_FIELD, paper.idConference, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_IDJOURNAL_FIELD, paper.idJournal, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_ISBN_FIELD, paper.isbn, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.PAPER_JOURNALNAME_FIELD, paper.journalName, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                d.add(new Field(IndexConst.PAPER_PAGES_FIELD, paper.pages, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.PAPER_VOLUME_FIELD, paper.volume, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.PAPER_VIEWPUBLICATION_FIELD, paper.viewPublication, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.PAPER_AUTHORS_FIELD, paper.authors, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                d.add(new Field(IndexConst.PAPER_AUTHORSNAME_FIELD, paper.authorsName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_KEYWORDSNAME_FIELD, paper.keywordsName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTCITATION_FIELD, paper.listCitation, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.PAPER_LISTIDAUTHOR_FIELD, paper.listIdAuthor, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTIDKEYWORD_FIELD, paper.listIdKeyword, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTIDORG_FIELD, paper.listIdOrg, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTIDSUBDOMAIN_FIELD, paper.listIdSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.PAPER_LISTIDPAPERCITATION_FIELD, paper.listIdPaperCitation, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new NumericField(IndexConst.PAPER_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(paper.citationCount));
                d.add(new NumericField(IndexConst.PAPER_REFRENCECOUNT_FIELD, Field.Store.YES, true).setIntValue(paper.referenceCount));
                d.add(new NumericField(IndexConst.PAPER_YEAR_FIELD, Field.Store.YES, true).setIntValue(paper.year));
                d.add(new NumericField(IndexConst.PAPER_RANK_FIELD, Field.Store.YES, true).setIntValue(paper.rank));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + paper.title);
                d = null;
                paper = null;
            }
            //count = writer.numDocs();
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            writer.optimize();
            writer.close();
            directory.close();
        }
        return count;
    }

    /*
     * @pararam idPaper
     * @return ConferenceName
     */
    private String getConferenceName(ConnectionPool connectionPool, int idPaper) throws SQLException, ClassNotFoundException {
        String name = "";
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT c." + ConferenceTB.COLUMN_CONFERENCENAME + " FROM " + PaperTB.TABLE_NAME + " p JOIN " + ConferenceTB.TABLE_NAME + " c ON c." + ConferenceTB.COLUMN_CONFERENCEID + " = p." + PaperTB.COLUMN_CONFERENCEID + " WHERE p." + PaperTB.COLUMN_PAPERID + " = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idPaper);
            ResultSet rs = stmt.executeQuery();
            if ((rs != null) && (rs.next())) {
                name = rs.getString(ConferenceTB.COLUMN_CONFERENCENAME);
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return name;
    }

    /*
     * @pararam idPaper
     * @return getJournalName
     */
    private String getJournalName(ConnectionPool connectionPool, int idPaper) throws SQLException, ClassNotFoundException {
        String name = "";
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT j." + JournalTB.COLUMN_JOURNALNAME + " FROM " + PaperTB.TABLE_NAME + " p JOIN " + JournalTB.TABLE_NAME + " j ON j." + JournalTB.COLUMN_JOURNALID + " = p." + PaperTB.COLUMN_JOURNALID + " WHERE p." + PaperTB.COLUMN_PAPERID + " = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idPaper);
            ResultSet rs = stmt.executeQuery();
            if ((rs != null) && (rs.next())) {
                name = rs.getString(JournalTB.COLUMN_JOURNALNAME);
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return name;

    }

    /* 
     * truy vấn thông tin về các tác giả tham gia viết paper
     * @param idPaper
     * @return authors {idAuthor, authorName}, authorsName, listIdAuthor, listIdOrg, orgsName
     */
    private LinkedHashMap<String, String> getListAuthor(ConnectionPool connectionPool, int idPaper) throws SQLException, ClassNotFoundException {
        LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
        Map json = new HashMap();
        ArrayList<Object> authors = new ArrayList<Object>();
        String authorsName = "";
        String listIdAuthor = "";
        String listIdOrg = "";
        String orgsName = "";
        ArrayList<String> ArrayListIdOrg = new ArrayList<String>();
        try {
            Connection connection = connectionPool.getConnection();
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
                orgsName += " " + rs.getString(OrgTB.COLUMN_ORGNAME);
                listIdAuthor += " " + rs.getString(AuthorTB.COLUMN_AUTHORID);
                if ((rs.getInt(AuthorTB.COLUMN_ORGID) > 0) && (!ArrayListIdOrg.contains(rs.getString(AuthorTB.COLUMN_ORGID)))) {
                    ArrayListIdOrg.add(rs.getString(AuthorTB.COLUMN_ORGID));
                }
                authors.add(author);
            }
            for (int i = 0; i < ArrayListIdOrg.size(); i++) {
                listIdOrg += " " + ArrayListIdOrg.get(i);
            }
            if (!"".equals(authorsName)) {
                authorsName = authorsName.substring(1);
            }
            if (!"".equals(listIdAuthor)) {
                listIdAuthor = listIdAuthor.substring(1);
            }
            if (!"".equals(listIdOrg)) {
                listIdOrg = listIdOrg.substring(1);
            }
            if (!"".equals(orgsName)) {
                orgsName = orgsName.substring(1);
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        json.put("authors", authors);
        JSONObject outJSON = new JSONObject(json);
        result.put("authors", outJSON.toJSONString());
        result.put("authorsName", authorsName);
        result.put("listIdAuthor", listIdAuthor);
        result.put("listIdOrg", listIdOrg);
        result.put("orgsName", orgsName);
        return result;
    }

    /*
     * truy vấn thông tin về citation theo thời gian
     * @param idPaper
     * @return citationCount, listCitation
     * listCitation: ArrayList<Object> listCitation = new ArrayList<Object>();
     * + Với: Object là LinkedHashMap<String, Integer> với liệu là {citation, year}
     */
    private LinkedHashMap<String, String> getListCitation(ConnectionPool connectionPool, int idPaper) throws SQLException, ClassNotFoundException {
        LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
        ArrayList<Object> listCitation = new ArrayList<Object>();
        int citationCount = 0;
        try {
            Connection connection = connectionPool.getConnection();
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
                    listCitation.add(temp);
                }
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        result.put("listCitation", Common.OToS(listCitation));
        result.put("citationCount", Integer.toString(citationCount));
        return result;
    }

    /*
     * @pararam idPaper
     * @return listSubdomain
     */
    private String getListIdSubdomains(ConnectionPool connectionPool, int idPaper) throws SQLException, ClassNotFoundException {
        String list = "";
        try {
            Connection connection = connectionPool.getConnection();
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
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return list;
    }

    /*
     * @pararam idPaper
     * @return list idKeyword and keyword
     */
    private LinkedHashMap<String, String> getListKeyword(ConnectionPool connectionPool, int idPaper) throws SQLException, ClassNotFoundException {
        LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
        String listIdKeyword = "";
        String keywords = "";
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT pk." + PaperKeywordTB.COLUMN_KEYWORDID + ", (SELECT k." + KeywordTB.COLUMN_KEYWORD + " FROM " + KeywordTB.TABLE_NAME + " k WHERE pk." + PaperKeywordTB.COLUMN_KEYWORDID + " = k." + KeywordTB.COLUMN_KEYWORDID + ") AS " + KeywordTB.COLUMN_KEYWORD + " FROM " + PaperKeywordTB.TABLE_NAME + " pk WHERE pk." + PaperKeywordTB.COLUMN_PAPERID + " = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idPaper);
            ResultSet rs = stmt.executeQuery();
            while ((rs != null) && (rs.next())) {
                listIdKeyword += " " + rs.getString(PaperKeywordTB.COLUMN_KEYWORDID);
                keywords += " " + rs.getString(KeywordTB.COLUMN_KEYWORD);
            }
            if (!"".equals(listIdKeyword)) {
                listIdKeyword = listIdKeyword.substring(1);
            }
            if (!"".equals(keywords)) {
                keywords = keywords.substring(1);
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        result.put("listIdKeyword", listIdKeyword);
        result.put("keywords", keywords);
        return result;
    }

    /*
     * @pararam idPaper
     * @return rank of paper
     */
    private int getRank(ConnectionPool connectionPool, int idPaper) throws SQLException, ClassNotFoundException {
        int rank = 0;
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT r." + RankPaperTB.COLUMN_RANK + " FROM " + RankPaperTB.TABLE_NAME + " r WHERE r." + RankPaperTB.COLUMN_PAPERID + " = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idPaper);
            ResultSet rs = stmt.executeQuery();
            if ((rs != null) && (rs.next())) {
                rank = rs.getInt(RankPaperTB.COLUMN_RANK);
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return rank;
    }

    /*
     * @pararam idPaper
     * @return list idPaper citation
     */
    private String getListIdPaperCitations(ConnectionPool connectionPool, int idPaper) throws SQLException {
        String listIdPaperCitations = "";
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT pp." + PaperPaperTB.COLUMN_PAPERID + " FROM " + PaperPaperTB.TABLE_NAME + " pp WHERE pp." + PaperPaperTB.COLUMN_PAPERREFID + " = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idPaper);
            ResultSet rs = stmt.executeQuery();
            while ((rs != null) && (rs.next())) {
                listIdPaperCitations += " " + rs.getString(PaperPaperTB.COLUMN_PAPERID);
            }
            if (!"".equals(listIdPaperCitations)) {
                listIdPaperCitations = listIdPaperCitations.substring(1);
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return listIdPaperCitations;
    }

    /*
     * @pararam idPaper
     * @return ReferenceCount
     */
    private int getReferenceCount(ConnectionPool connectionPool, int idPaper) throws SQLException {
        int count = 0;
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT COUNT(p." + PaperPaperTB.COLUMN_PAPERREFID + ") AS total FROM " + PaperPaperTB.TABLE_NAME + " p WHERE p." + PaperPaperTB.COLUMN_PAPERID + " = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idPaper);
            ResultSet rs = stmt.executeQuery();
            if ((rs != null) && (rs.next())) {
                count += rs.getInt("total");
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return count;
    }

    /**
     * hàm test index
     *
     * @param args
     */
    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String user = "root";
            String pass = "root";
            String database = "cspublicationcrawler1";
            int port = 3306;
            String path = "E:\\INDEX\\";
            ConnectionPool connectionPool = new ConnectionPool(user, pass, database, port);
            PaperIndexer indexer = new PaperIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}