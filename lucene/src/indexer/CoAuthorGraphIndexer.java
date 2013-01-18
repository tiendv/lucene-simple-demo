/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.IndexConst;
import dto.CoAuthorEdgeDTO;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Date;
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
 * @author DaoLV
 */
public class CoAuthorGraphIndexer {

    private String path = "D:\\DaoLVData\\INDEX\\";

    public CoAuthorGraphIndexer(String path) {
        try {
            this.path = path;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
    
    public String _run(String filePath) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.COAUTHORGRAPH_DIRECTORY_PATH);
            long start = new Date().getTime();
            int count = this._index(filePath, indexDir);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }
    
    private int _index(String filePath, File indexDir) {
        int count = 0;
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            
            FileInputStream fos = new FileInputStream(filePath);
            Reader r = new InputStreamReader(fos,"UTF8");
            BufferedReader reader = new BufferedReader(r);
            String line = null;
            reader.readLine(); // ignore first row
            String[] tokens;
            String preAuthorID1="";
            String authorID1 ;
            String authorID2;
            CoAuthorEdgeDTO edgeDTO = null;
            while((line=reader.readLine()) != null)
            {
                tokens = line.split("\t");
                authorID1 = tokens[0];
                authorID2 = tokens[1];
                if (!preAuthorID1.equals(authorID1))
                {
                    if (edgeDTO != null)
                    {
                        Document d = new Document();
                        d.add(new Field(IndexConst.COAUTHORGRAPH_IDAUTHOR1_FIELD, edgeDTO.idAuthor1, Field.Store.YES, Field.Index.ANALYZED));
                        d.add(new Field(IndexConst.COAUTHORGRAPH_IDAUTHOR2_FIELD, edgeDTO.listIdAuthor2, Field.Store.YES, Field.Index.NO));
                        writer.addDocument(d);
                        System.out.println("Indexing : " + count++ + "\t");
                    }
                    
                    edgeDTO = new CoAuthorEdgeDTO();
                    edgeDTO.setIdAuthor1(authorID1);
                    preAuthorID1 = authorID1;
                }
                edgeDTO.addIdAuthor2(authorID2);
            }
            //Index the last author
            Document d = new Document();
            d.add(new Field(IndexConst.COAUTHORGRAPH_IDAUTHOR1_FIELD, edgeDTO.idAuthor1, Field.Store.YES, Field.Index.ANALYZED));
            d.add(new Field(IndexConst.COAUTHORGRAPH_IDAUTHOR2_FIELD, edgeDTO.listIdAuthor2, Field.Store.YES, Field.Index.NO));
            writer.addDocument(d);
            System.out.println("Indexing : " + count++ + "\t");
            //
            
            count = writer.numDocs();
            writer.optimize();
            writer.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return 0;
        }
        return count;
    }

    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String path = "D:\\DaoLVData\\INDEX\\";
            CoAuthorGraphIndexer indexer = new CoAuthorGraphIndexer(path);
            String filePath = "D:\\DaoLVData\\RTBVSGraphData\\RTBVSGraph.txt";
            System.out.println(indexer._run(filePath));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}