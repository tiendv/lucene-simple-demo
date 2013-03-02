/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package searcher;

import constant.Common;
import constant.IndexConst;
import constant.PubCiComparator;
import dto.PubCiDTO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.json.simple.JSONObject;

/**
 *
 * @author HuyDang
 */
public class PaperSearcher {

    private static IndexSearcher searcher = null;

    public static IndexSearcher getSearcher(String path) {
        try {
            if (searcher == null) {
                searcher = new IndexSearcher(Common.getFSDirectory(path, IndexConst.PAPER_INDEX_PATH));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return searcher;
    }

    public void destroy() throws IOException {
        searcher.close();
    }

    /**
     * Tính toán các thông tin publication, ciatation theo từng năm của một
     *
     * @param path (path lucene index)
     * @param querySearch
     * @param
     * field//1:idAuthor||2:idConference||3:idJournal||4:idOrg||5:idKeyword||6:Subdomain
     * @return Map chứa thông tin publication, ciatation theo từng năm của một
     * querySearch
     */
    public LinkedHashMap<String, String> getListPublicationCitation(String path, String querySearch, int field) throws IOException {
        LinkedHashMap<String, String> out = new LinkedHashMap<String, String>();
        try {
            BooleanQuery blQuery = new BooleanQuery();
            // querySearch
            if (field == 2) {// Conference
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDCONFERENCE_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(querySearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else if (field == 3) {//Journal
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDJOURNAL_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(querySearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else if (field == 4) {// Org
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDORG_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(querySearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else if (field == 5) {// Keyword
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDKEYWORD_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(querySearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else if (field == 6) {// Subdomain
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(querySearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else {
                return out;
            }
            TopDocs result = getSearcher(path).search(blQuery, Integer.MAX_VALUE);
            if (result != null) {
                ScoreDoc[] hits = result.scoreDocs;
                ArrayList<PubCiDTO> pubCiDTOList = new ArrayList<PubCiDTO>();
                int citationCount = 0;
                for (int i = 0; i < result.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = getSearcher(path).doc(hit.doc);
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
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return out;
    }

    /**
     * @param path (path lucene index)
     * @param idSearch
     * @param last
     * @param
     * field//1:idAuthor||2:idConference||3:idJournal||4:idOrg||5:idKeyword
     * @return publicationCount, citationCount and publicationList(for H-Index
     * and G-Index) for last
     * @throws Exception
     */
    public LinkedHashMap<String, Object> getPapersForAll(String path, String idSearch, int last, int field) throws Exception {
        LinkedHashMap<String, Object> out = new LinkedHashMap<String, Object>();
        try {
            BooleanQuery blQuery = new BooleanQuery();
            Sort sort = null;
            TopDocs topDocs = null;
            // querySearch
            if (field == 1) {// Author
                QueryParser parserAuthor = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHOR_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryAuthor = parserAuthor.parse(idSearch);
                blQuery.add(queryAuthor, BooleanClause.Occur.MUST);
            } else if (field == 2) {// Conference
                QueryParser parserConference = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDCONFERENCE_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryConference = parserConference.parse(idSearch);
                blQuery.add(queryConference, BooleanClause.Occur.MUST);
            } else if (field == 3) {//Journal
                QueryParser parserJournal = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDJOURNAL_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryJournal = parserJournal.parse(idSearch);
                blQuery.add(queryJournal, BooleanClause.Occur.MUST);
            } else if (field == 4) {// Org
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDORG_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(idSearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else if (field == 5) {// Keyword
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDKEYWORD_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(idSearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else {
                return out;
            }
            // Sort
            sort = new Sort(new SortField[]{
                        new SortField(IndexConst.PAPER_CITATIONCOUNT_FIELD, SortField.INT, true)});
            // Year
            if (last > 0) {
                Calendar calendar = GregorianCalendar.getInstance();
                calendar.setTime(new Date());
                int lastYear = calendar.get(Calendar.YEAR) - last;
                int currentYear = calendar.get(Calendar.YEAR);
                Filter filter = NumericRangeFilter.newIntRange(IndexConst.PAPER_YEAR_FIELD, lastYear, currentYear, true, true);
                topDocs = getSearcher(path).search(blQuery, filter, Integer.MAX_VALUE, sort);
            } else {
                topDocs = getSearcher(path).search(blQuery, Integer.MAX_VALUE, sort);
            }
            if (topDocs != null) {
                ArrayList<Integer> publicationList = new ArrayList<Integer>();
                int citationCount = 0;
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = getSearcher(path).doc(hit.doc);
                    citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                    publicationList.add(Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD)));
                }
                out.put("pubCount", topDocs.totalHits);
                out.put("citCount", citationCount);
                out.put("list", publicationList);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return out;
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @param last
     * @return publicationCount, citationCount for last
     */
    public LinkedHashMap<String, Integer> getPublicationsFromIdSubdomain(String path, String idSubdomain, int last) throws Exception {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<String, Integer>();
        try {
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = null;
            if (last > 0) {
                Calendar calendar = GregorianCalendar.getInstance();
                calendar.setTime(new Date());
                int lastYear = calendar.get(Calendar.YEAR) - last;
                int currentYear = calendar.get(Calendar.YEAR);
                Filter filter = NumericRangeFilter.newIntRange(IndexConst.PAPER_YEAR_FIELD, lastYear, currentYear, true, true);
                topDocs = getSearcher(path).search(blQuery, filter, Integer.MAX_VALUE);
            } else {
                topDocs = getSearcher(path).search(blQuery, Integer.MAX_VALUE);
            }
            if (topDocs != null && topDocs.totalHits > 0) {
                int citationCout = 0;
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = getSearcher(path).doc(hit.doc);
                    citationCout += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                }
                out.put("pubCount", topDocs.totalHits);
                out.put("citCount", citationCout);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return out;
    }

    /**
     * lấy chuỗi các bài viết với citation của bài viết đó được sắp xếp từ cao
     * xuống thấp
     *
     * @param idSearch
     * @return ArrayList lưu citation từ cao xuống thấp
     */
    public ArrayList<Integer> getPublicationList(String path, String idSearch, int field) throws IOException, ParseException {
        ArrayList<Integer> publicationList = new ArrayList<Integer>();
        try {
            BooleanQuery blQuery = new BooleanQuery();
            if (field == 1) {//Author
                QueryParser parserAuthor = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHOR_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryAuthor = parserAuthor.parse(idSearch);
                blQuery.add(queryAuthor, BooleanClause.Occur.MUST);
            } else if (field == 2) {// Conference                
                QueryParser parserConference = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDCONFERENCE_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryConference = parserConference.parse(idSearch);
                blQuery.add(queryConference, BooleanClause.Occur.MUST);
            } else if (field == 3) {//Journal
                QueryParser parserJournal = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDJOURNAL_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryJournal = parserJournal.parse(idSearch);
                blQuery.add(queryJournal, BooleanClause.Occur.MUST);
            } else if (field == 4) {// Org
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDORG_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(idSearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else {
                return publicationList;
            }
            Sort sort = new Sort(new SortField[]{
                        new SortField(IndexConst.PAPER_CITATIONCOUNT_FIELD, SortField.INT, true)});
            TopDocs result = getSearcher(path).search(blQuery, Integer.MAX_VALUE, sort);
            if (result != null) {
                ScoreDoc[] hits = result.scoreDocs;
                for (int i = 0; i < result.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = getSearcher(path).doc(hit.doc);
                    publicationList.add(Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD)));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return publicationList;
    }

    /**
     * Tính toán Map với các thông tin publication, ciatation theo từng năm của
     * một idSearch đối với một subdomain
     *
     * @param path (path lucene index)
     * @param idSearch
     * @param idSubdomain
     * @param
     * field//1:idAuthor||2:idConference||3:idJournal||4:idOrg||5:idKeyword
     */
    public String getChartFromIdSubdomain(String path, String idSubdomain, String idSearch, int field) {
        String out = "";
        try {
            BooleanQuery blQuery = new BooleanQuery();
            // idSubdomain
            QueryParser parserSub = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query querySub = parserSub.parse(idSubdomain);
            blQuery.add(querySub, BooleanClause.Occur.MUST);
            // idSearch
            if (field == 2) {// Conference
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDCONFERENCE_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(idSearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else if (field == 3) {//Journal
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDJOURNAL_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(idSearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else if (field == 4) {// Org
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDORG_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(idSearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else if (field == 5) {// Keyword
                QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDKEYWORD_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query query = parser.parse(idSearch);
                blQuery.add(query, BooleanClause.Occur.MUST);
            } else {
                return null;
            }
            TopDocs result = getSearcher(path).search(blQuery, Integer.MAX_VALUE);
            // Result
            ArrayList<Object> listPublicationCitation = new ArrayList<Object>();
            if (result != null && result.totalHits > 0) {
                ScoreDoc[] hits = result.scoreDocs;
                ArrayList<PubCiDTO> pubCiDTOList = new ArrayList<PubCiDTO>();
                for (int i = 0; i < result.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = getSearcher(path).doc(hit.doc);
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
                for (int i = 0; i < pubCiDTOList.size(); i++) {
                    LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
                    temp.put("publication", pubCiDTOList.get(i).getPublication());
                    temp.put("citation", pubCiDTOList.get(i).getCitation());
                    temp.put("year", pubCiDTOList.get(i).getYear());
                    listPublicationCitation.add(temp);
                }
            }
            Map json = new HashMap();
            json.put("listPublicationCitation", listPublicationCitation);
            JSONObject outJSON = new JSONObject(json);
            out = outJSON.toJSONString();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return out;
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @param querySearch
     * @param last
     * @param
     * field//1:idAuthor||2:idConference||3:idJournal||4:idOrg||5:idKeyword
     * @return publicationCount, citationCount and publicationList(for H-Index
     * and G-Index) for last
     * @throws Exception
     */
    public LinkedHashMap<String, Object> getPapersForRankSubDomain(String path, String idSubdomain, String querySearch, int last, int field) throws Exception {
        LinkedHashMap<String, Object> out = new LinkedHashMap<String, Object>();
        try {
            BooleanQuery blQuery = new BooleanQuery();
            Sort sort = null;
            TopDocs topDocs = null;
            // querySearch
            if (field == 1) {// Author
                QueryParser parserAuthor = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHOR_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryAuthor = parserAuthor.parse(querySearch);
                blQuery.add(queryAuthor, BooleanClause.Occur.MUST);
            } else if (field == 2) {// Conference
                QueryParser parserConference = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDCONFERENCE_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryConference = parserConference.parse(querySearch);
                blQuery.add(queryConference, BooleanClause.Occur.MUST);
            } else if (field == 3) {//Journal
                QueryParser parserJournal = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDJOURNAL_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryJournal = parserJournal.parse(querySearch);
                blQuery.add(queryJournal, BooleanClause.Occur.MUST);
            } else if (field == 4) {// Org
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDORG_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(querySearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else if (field == 5) {// Keyword
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDKEYWORD_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(querySearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else {
                return null;
            }
            // Subdomain
            QueryParser parserSubdomain = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query querySubdomain = parserSubdomain.parse(idSubdomain);
            blQuery.add(querySubdomain, BooleanClause.Occur.MUST);
            // Sort
            sort = new Sort(new SortField[]{
                        new SortField(IndexConst.PAPER_CITATIONCOUNT_FIELD, SortField.INT, true)});
            // Year
            if (last > 0) {
                Calendar calendar = GregorianCalendar.getInstance();
                calendar.setTime(new Date());
                int lastYear = calendar.get(Calendar.YEAR) - last;
                int currentYear = calendar.get(Calendar.YEAR);
                Filter filter = NumericRangeFilter.newIntRange(IndexConst.PAPER_YEAR_FIELD, lastYear, currentYear, true, true);
                topDocs = getSearcher(path).search(blQuery, filter, Integer.MAX_VALUE, sort);
            } else {
                topDocs = getSearcher(path).search(blQuery, Integer.MAX_VALUE, sort);
            }
            if (topDocs != null && topDocs.totalHits > 0) {
                ArrayList<Integer> publicationList = new ArrayList<Integer>();
                int citationCount = 0;
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = getSearcher(path).doc(hit.doc);
                    citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                    publicationList.add(Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD)));
                }
                out.put("pubCount", topDocs.totalHits);
                out.put("citCount", citationCount);
                out.put("list", publicationList);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return out;
    }
}