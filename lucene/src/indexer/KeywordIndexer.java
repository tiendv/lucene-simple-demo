/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import bo.IndexBO;
import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import constant.PubCiComparator;
import database.KeywordTB;
import database.PaperKeywordTB;
import database.SubdomainPaperTB;
import dto.KeywordDTO;
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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class KeywordIndexer {

    private IndexSearcher searcher = null;
    private String path = "E:\\";

    public KeywordIndexer(String path) {
        try {
            FSDirectory directory = Common.getFSDirectory(path, IndexConst.PAPER_INDEX_PATH);
            searcher = new IndexSearcher(directory);
            this.path = path;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.KEYWORD_INDEX_PATH);
            long start = new Date().getTime();
            int count = this._index(connectionPool, indexDir);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    private int _index(ConnectionPool connectionPool, File indexDir) {
        int count = 0;
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            // Connection to DB
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT * FROM " + KeywordTB.TABLE_NAME + " k limit 0,10";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            IndexBO indexBO = new IndexBO();
            KeywordDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new KeywordDTO();
                LinkedHashMap<String, String> listPublicationCitation = this.getListPublicationCitation(rs.getString(KeywordTB.COLUMN_KEYWORDID));
                dto.setIdKeyword(rs.getString(KeywordTB.COLUMN_KEYWORDID));
                dto.setKeyword(rs.getString(KeywordTB.COLUMN_KEYWORD));
                dto.setStemmingVariations(rs.getString(KeywordTB.COLUMN_STEMMINGVARIATIONS));
                dto.setListIdSubdomain(this.getListIdSubdomain(connectionPool, rs.getInt(KeywordTB.COLUMN_KEYWORDID)));
                dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));

                int pubLast5Year = 0;
                int citLast5Year = 0;
                int pubLast10Year = 0;
                int citLast10Year = 0;

                LinkedHashMap<String, Object> object10Year = indexBO.getPapersForAll(path + IndexConst.PAPER_INDEX_PATH, rs.getString(KeywordTB.COLUMN_KEYWORDID), 10, 5);
                if (object10Year != null) {
                    pubLast10Year = Integer.parseInt(object10Year.get("pubCount").toString());
                    citLast10Year = Integer.parseInt(object10Year.get("citCount").toString());

                    LinkedHashMap<String, Object> object5Year = indexBO.getPapersForAll(path + IndexConst.PAPER_INDEX_PATH, rs.getString(KeywordTB.COLUMN_KEYWORDID), 5, 5);
                    if (object5Year != null) {
                        pubLast5Year = Integer.parseInt(object5Year.get("pubCount").toString());
                        citLast5Year = Integer.parseInt(object5Year.get("citCount").toString());
                    }
                }

                Document d = new Document();
                d.add(new Field(IndexConst.KEYWORD_IDKEYWORD_FIELD, dto.idKeyword, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.KEYWORD_KEYWORD_FIELD, dto.keyword, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.KEYWORD_STEMMINGVARIATIONS_FIELD, dto.stemmingVariations, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.KEYWORD_LISTIDSUBDOMAIN_FIELD, dto.listIdSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.KEYWORD_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.NO));

                d.add(new NumericField(IndexConst.KEYWORD_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.publicationCount));
                d.add(new NumericField(IndexConst.KEYWORD_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.citationCount));

                d.add(new NumericField(IndexConst.KEYWORD_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                d.add(new NumericField(IndexConst.KEYWORD_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                d.add(new NumericField(IndexConst.KEYWORD_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                d.add(new NumericField(IndexConst.KEYWORD_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.keyword);
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

    private String getListIdSubdomain(ConnectionPool connectionPool, int idKeyword) throws SQLException, ClassNotFoundException {
        String list = "";
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + " FROM " + PaperKeywordTB.TABLE_NAME + " pk JOIN " + SubdomainPaperTB.TABLE_NAME + " sp ON sp." + SubdomainPaperTB.COLUMN_PAPERID + " = pk." + PaperKeywordTB.COLUMN_PAPERID + " WHERE pk." + PaperKeywordTB.COLUMN_KEYWORDID + " = ? GROUP BY sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + "";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idKeyword);
            ResultSet rs = stmt.executeQuery();
            while ((rs != null) && (rs.next())) {
                list += " " + rs.getString(SubdomainPaperTB.COLUMN_SUBDOMAINID);
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

    private LinkedHashMap<String, String> getListPublicationCitation(String idKeyword) throws IOException, ParseException {
        LinkedHashMap<String, String> out = new LinkedHashMap<String, String>();
        BooleanQuery booleanQuery = new BooleanQuery();
        QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDKEYWORD_FIELD, new StandardAnalyzer(Version.LUCENE_36));
        Query query = parser.parse(idKeyword);
        booleanQuery.add(query, BooleanClause.Occur.MUST);
        TopDocs result = searcher.search(booleanQuery, Integer.MAX_VALUE);
        if (result != null) {
            ScoreDoc[] hits = result.scoreDocs;
            ArrayList<PubCiDTO> pubCiDTOList = new ArrayList<PubCiDTO>();
            int citationCount = 0;
            for (int i = 0; i < result.totalHits; i++) {
                ScoreDoc hit = hits[i];
                Document doc = searcher.doc(hit.doc);
                citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                ArrayList<Object> listCitations = (ArrayList<Object>) Common.SToO(doc.get(IndexConst.PAPER_LISTCITATION_FIELD));
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
            out.put("publicationCount", Integer.toString(result.totalHits));
            out.put("citationCount", Integer.toString(citationCount));
            out.put("listPublicationCitation", Common.OToS(listPublicationCitation));
        }
        return out;
    }

    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String user = "root";
            String pass = "root";
            String database = "cspublicationcrawler";
            int port = 3306;
            String path = "C:\\";
            ConnectionPool connectionPool = new ConnectionPool(user, pass, database, port);
            KeywordIndexer indexer = new KeywordIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}