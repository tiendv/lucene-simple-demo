/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import constant.PubCiComparator;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.SubdomainPaperTB;
import dto.AuthorDTO;
import dto.PubCiDTO;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class AuthorIndexer {

    private ConnectionPool connectionPool;
    private IndexSearcher searcher = null;
    private String path = null;
    public Boolean connect = true;
    public Boolean folder = true;

    public AuthorIndexer(String username, String password, String database, int port, String path) {
        try {
            FSDirectory directory = Common.getFSDirectory(path, IndexConst.PAPER_INDEX_PATH);
            if (directory == null) {
                folder = false;
            }
            this.path = path;
            searcher = new IndexSearcher(directory);
            connectionPool = new ConnectionPool(username, password, database, port);
            if (connectionPool.getConnection() == null) {
                connect = false;
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    public String _run() {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.AUTHOR_INDEX_PATH);
            long start = new Date().getTime();
            int count = this._index(indexDir, connectionPool);
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
            String sql = "SELECT * FROM " + AuthorTB.TABLE_NAME + " a";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            AuthorDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new AuthorDTO();
                Document d = new Document();
                LinkedHashMap<String, String> listPublicationCitation = this.getListPublicationCitation(rs.getString(AuthorTB.COLUMN_AUTHORID));
                LinkedHashMap<String, Integer> indexAuthor = this.getCalculateIndexAuthor(rs.getString(AuthorTB.COLUMN_AUTHORID));
                LinkedHashMap<String, String> listIdSubdomains = this.getListIdSubdomains(rs.getInt(AuthorTB.COLUMN_AUTHORID));
                dto.setIdAuthor(rs.getString(AuthorTB.COLUMN_AUTHORID));
                dto.setAuthorName(rs.getString(AuthorTB.COLUMN_AUTHORNAME));
                dto.setIdOrg(rs.getString(AuthorTB.COLUMN_ORGID));
                dto.setImage(rs.getString(AuthorTB.COLUMN_IMAGE));
                dto.setWebsite(rs.getString(AuthorTB.COLUMN_WEBSITE));
                dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                dto.setCoAuthorCount(Integer.parseInt(listPublicationCitation.get("coAuthorCount")));
                dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));
                dto.setListIdSubdomains(listIdSubdomains.get("listIdSubdomains"));
                dto.setListRankSubdomain(listIdSubdomains.get("listRankSubdomain"));
                dto.setH_Index(indexAuthor.get("h_index"));
                dto.setG_Index(indexAuthor.get("g_index"));
                dto.setRank(0);

                d.add(new Field(IndexConst.AUTHOR_AUTHORNAME_FIELD, dto.authorName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_IDAUTHOR_FIELD, dto.idAuthor, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_IDORG_FIELD, dto.idOrg, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_IMAGE_FIELD, dto.image, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.AUTHOR_WEBSITE_FIELD, dto.website, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_LISTIDSUBDOMAINS_FIELD, dto.listIdSubdomains, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_LISTRANKSUBDOMAIN_FIELD, dto.listRankSubdomain, Field.Store.YES, Field.Index.NO));
                d.add(new NumericField(IndexConst.AUTHOR_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.citationCount));
                d.add(new NumericField(IndexConst.AUTHOR_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.publicationCount));
                d.add(new NumericField(IndexConst.AUTHOR_HINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.h_index));
                d.add(new NumericField(IndexConst.AUTHOR_GINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.g_index));
                d.add(new NumericField(IndexConst.AUTHOR_COAUTHORCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.coAuthorCount));
                d.add(new NumericField(IndexConst.AUTHOR_RANK_FIELD, Field.Store.YES, true).setIntValue(dto.rank));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.authorName);
                d = null;
                dto = null;
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

    /*
     * getListIdSubdomains
     * @param idAuthor
     * @return listIdSubdomains, listRankSubdomain {idSubdomain {publicationCount, citationCount, coAuthorCount, h_index, g_index}}
     */
    public LinkedHashMap<String, String> getListIdSubdomains(int idAuthor) throws SQLException, ClassNotFoundException, IOException, ParseException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        LinkedHashMap<String, String> out = new LinkedHashMap<String, String>();
        String listIdSubdomains = "";
        LinkedHashMap<Integer, Object> listRankSubdomain = new LinkedHashMap<Integer, Object>();
        String sql = "SELECT sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + " FROM " + AuthorPaperTB.TABLE_NAME + " ap JOIN " + SubdomainPaperTB.TABLE_NAME + " sp ON ap." + AuthorPaperTB.COLUMN_PAPERID + " = sp." + SubdomainPaperTB.COLUMN_PAPERID + " WHERE ap." + AuthorPaperTB.COLUMN_AUTHORID + " = ? GROUP BY sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + "";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, idAuthor);
        ResultSet rs = stmt.executeQuery();
        while ((rs != null) && (rs.next())) {
            listIdSubdomains += " " + rs.getString(SubdomainPaperTB.COLUMN_SUBDOMAINID);
            listRankSubdomain.put(rs.getInt(SubdomainPaperTB.COLUMN_SUBDOMAINID), this.getPublicationByIdAuthorAndIdSubdomain(Integer.toString(idAuthor), rs.getString(SubdomainPaperTB.COLUMN_SUBDOMAINID)));
        }
        if (!"".equals(listIdSubdomains)) {
            listIdSubdomains = listIdSubdomains.substring(1);
        }
        stmt.close();
        connection.close();
        out.put("listIdSubdomains", listIdSubdomains);
        out.put("listRankSubdomain", Common.OToS(listRankSubdomain));
        return out;
    }

    /*
     * getListPublicationCitation
     * @param idAuthor
     * @return publicationCount, citationCount, coAuthorCount, pubCiDTOList{publication, citation, year}
     */
    public LinkedHashMap<String, String> getListPublicationCitation(String idAuthor) throws IOException, ParseException, SQLException, ClassNotFoundException {
        LinkedHashMap<String, String> out = new LinkedHashMap<String, String>();
        BooleanQuery booleanQuery = new BooleanQuery();
        QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHORS_FIELD, new StandardAnalyzer(Version.LUCENE_36));
        Query query = parser.parse(idAuthor);
        booleanQuery.add(query, BooleanClause.Occur.MUST);
        TopDocs result = searcher.search(booleanQuery, Integer.MAX_VALUE);
        if (result != null) {
            ScoreDoc[] hits = result.scoreDocs;
            ArrayList<PubCiDTO> pubCiDTOList = new ArrayList<PubCiDTO>();
            int citationCount = 0;
            int coAuthorCount = 0;
            String listIdPaper = "";
            for (int i = 0; i < result.totalHits; i++) {
                ScoreDoc hit = hits[i];
                Document doc = searcher.doc(hit.doc);
                citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                listIdPaper += "," + doc.get(IndexConst.PAPER_IDPAPER_FIELD);
                ArrayList<Object> listCitations = (ArrayList<Object>) Common.SToO(doc.get(IndexConst.PAPER_LISTCITATIONS_FIELD));
                Iterator it = listCitations.iterator();
                while (it.hasNext()) {
                    LinkedHashMap<String, Integer> temp = (LinkedHashMap<String, Integer>) it.next();
                    if (pubCiDTOList.isEmpty()) {
                        PubCiDTO dto = new PubCiDTO();
                        dto.setCitation(temp.get("citation"));
                        dto.setPublication(0);
                        dto.setYear(temp.get("year"));
                        pubCiDTOList.add(dto);
                    } else {
                        Boolean flag = true;
                        for (int j = 0; j < pubCiDTOList.size(); j++) {
                            if (temp.get("year") == pubCiDTOList.get(j).getYear()) {
                                pubCiDTOList.get(j).setCitation(pubCiDTOList.get(j).getCitation() + temp.get("citation"));
                                flag = false;
                                break;
                            }
                        }
                        if (flag) {
                            PubCiDTO dto = new PubCiDTO();
                            dto.setCitation(temp.get("citation"));
                            dto.setPublication(0);
                            dto.setYear(temp.get("year"));
                            pubCiDTOList.add(dto);
                        }
                    }
                }
                if (Integer.parseInt(doc.get(IndexConst.PAPER_YEAR_FIELD)) == 0) {
                    continue;
                }
                if (pubCiDTOList.isEmpty()) {
                    PubCiDTO dto = new PubCiDTO();
                    dto.setCitation(0);
                    dto.setPublication(1);
                    dto.setYear(Integer.parseInt(doc.get(IndexConst.PAPER_YEAR_FIELD)));
                    pubCiDTOList.add(dto);
                } else {
                    Boolean flag = true;
                    for (int j = 0; j < pubCiDTOList.size(); j++) {
                        if (Integer.parseInt(doc.get(IndexConst.PAPER_YEAR_FIELD)) == pubCiDTOList.get(j).getYear()) {
                            pubCiDTOList.get(j).setPublication(pubCiDTOList.get(j).getPublication() + 1);
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        PubCiDTO dto = new PubCiDTO();
                        dto.setCitation(0);
                        dto.setPublication(1);
                        dto.setYear(Integer.parseInt(doc.get(IndexConst.PAPER_YEAR_FIELD)));
                        pubCiDTOList.add(dto);
                    }
                }
            }
            Collections.sort(pubCiDTOList, new PubCiComparator());
            ArrayList<Object> listPublicationCitation = new ArrayList<Object>();
            for (int i = 0; i < pubCiDTOList.size(); i++) {
                LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
                temp.put("publication", pubCiDTOList.get(i).getPublication());
                temp.put("citation", pubCiDTOList.get(i).getCitation());
                temp.put("year", pubCiDTOList.get(i).getYear());
                listPublicationCitation.add(temp);
            }
            if (!"".equals(listIdPaper)) {
                listIdPaper = listIdPaper.substring(1);
                coAuthorCount = this.getCoAuthorCount(listIdPaper);
            }
            out.put("publicationCount", Integer.toString(result.totalHits));
            out.put("citationCount", Integer.toString(citationCount));
            out.put("coAuthorCount", Integer.toString(coAuthorCount));
            out.put("listPublicationCitation", Common.OToS(listPublicationCitation));
        }
        return out;
    }

    /*
     * getCoAuthorCount
     * @param listIdPaper
     * @return
     */
    public int getCoAuthorCount(String listIdPaper) throws SQLException, ClassNotFoundException, IOException {
        Connection connection = ConnectionPool.dataSource.getConnection();
        int count = 0;
        String sql = "SELECT COUNT(DISTINCT ap." + AuthorPaperTB.COLUMN_AUTHORID + ") AS CoAuthorCount FROM " + AuthorPaperTB.TABLE_NAME + " ap WHERE ap." + AuthorPaperTB.COLUMN_PAPERID + " IN (" + listIdPaper + ")";
        PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        if ((rs != null) && (rs.next())) {
            count = rs.getInt("CoAuthorCount");
        }
        if (count > 0) {
            count = count - 1;
        }
        stmt.close();
        connection.close();
        return count;
    }

    /**
     * calculateIndexAuthor
     *
     * @throws Exception
     */
    public LinkedHashMap<String, Integer> getCalculateIndexAuthor(String idAuthor) throws Exception {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<String, Integer>();
        ArrayList<Integer> publicationList = this.getPublicationList(idAuthor);
        int h_index;
        int g_index;
        int citationCount;
        int citationCountSum;
        // Calculate h-index for each author.
        h_index = 0;
        while (h_index < publicationList.size()) {
            citationCount = publicationList.get(h_index);
            if (citationCount >= (h_index + 1)) {
                h_index++;
            } else {
                break;
            }
        }
        // Calculate g-index for each author.
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

    // Sắp xếp các bài viết theo thứ tự giảm dần số trích dẫn.
    public ArrayList<Integer> getPublicationList(String idAuthor) throws IOException, ParseException {
        ArrayList<Integer> publicationList = new ArrayList<Integer>();
        BooleanQuery booleanQuery = new BooleanQuery();
        QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHORS_FIELD, new StandardAnalyzer(Version.LUCENE_36));
        Query query = parser.parse(idAuthor);
        booleanQuery.add(query, BooleanClause.Occur.MUST);
        Sort sort = new Sort(new SortField[]{
                    new SortField(IndexConst.PAPER_CITATIONCOUNT_FIELD, SortField.INT, true)});
        TopDocs result = searcher.search(booleanQuery, Integer.MAX_VALUE, sort);
        if (result != null) {
            ScoreDoc[] hits = result.scoreDocs;
            for (int i = 0; i < result.totalHits; i++) {
                ScoreDoc hit = hits[i];
                Document doc = searcher.doc(hit.doc);
                publicationList.add(Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD)));
            }
        }
        return publicationList;
    }

    /*
     * getPublicationByIdAuthorAndIdSubdomain
     * @param idAuthor, idSubdomain
     * @return publicationCount, citationCount, coAuthorCount, h_index, g_index
     */
    public LinkedHashMap<String, Integer> getPublicationByIdAuthorAndIdSubdomain(String idAuthor, String idSubdomain) throws IOException, ParseException, SQLException, ClassNotFoundException {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<String, Integer>();
        ArrayList<Integer> publicationList = new ArrayList<Integer>();
        int publicationCount = 0;
        int citationCount = 0;
        int coAuthorCount = 0;
        int h_index;
        int g_index;
        int tempCitationCount;
        int tempCitationCountSum;
        String listIdPaper = "";
        // Query
        BooleanQuery booleanQuery = new BooleanQuery();
        QueryParser parserAuthor = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHORS_FIELD, new StandardAnalyzer(Version.LUCENE_36));
        Query queryAuthor = parserAuthor.parse(idAuthor);
        booleanQuery.add(queryAuthor, BooleanClause.Occur.MUST);
        QueryParser parserSubdomain = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDSUBDOMAINS_FIELD, new StandardAnalyzer(Version.LUCENE_36));
        Query querySubdomain = parserSubdomain.parse(idSubdomain);
        booleanQuery.add(querySubdomain, BooleanClause.Occur.MUST);
        Sort sort = new Sort(new SortField[]{
                    new SortField(IndexConst.PAPER_CITATIONCOUNT_FIELD, SortField.INT, true)});
        TopDocs result = searcher.search(booleanQuery, Integer.MAX_VALUE, sort);
        if (result != null) {
            ScoreDoc[] hits = result.scoreDocs;
            for (int i = 0; i < result.totalHits; i++) {
                ScoreDoc hit = hits[i];
                Document doc = searcher.doc(hit.doc);
                citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                listIdPaper += "," + doc.get(IndexConst.PAPER_IDPAPER_FIELD);
                publicationList.add(Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD)));
            }
            publicationCount = result.totalHits;
        }
        // Calculate h-index for each author.
        h_index = 0;
        while (h_index < publicationList.size()) {
            tempCitationCount = publicationList.get(h_index);
            if (tempCitationCount >= (h_index + 1)) {
                h_index++;
            } else {
                break;
            }
        }
        // Calculate g-index for each author.
        g_index = 0;
        tempCitationCountSum = 0;
        while (true) {
            if (g_index < publicationList.size()) {
                tempCitationCountSum += publicationList.get(g_index);
            }
            if (tempCitationCountSum >= ((g_index + 1) * (g_index + 1))) {
                g_index++;
            } else {
                break;
            }
        }
        // Calculate coAuthorCount for each author.
        if (!"".equals(listIdPaper)) {
            listIdPaper = listIdPaper.substring(1);
            coAuthorCount = this.getCoAuthorCount(listIdPaper);
        }
        // out
        out.put("publicationCount", publicationCount);
        out.put("citationCount", citationCount);
        out.put("coAuthorCount", coAuthorCount);
        out.put("h_index", h_index);
        out.put("g_index", g_index);
        return out;
    }

    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String user = "root";
            String pass = "@huydang1920@";
            String database = "cspublicationcrawler";
            int port = 3306;
            String path = "C:\\";
            AuthorIndexer indexer = new AuthorIndexer(user, pass, database, port, path);
            indexer._run();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}