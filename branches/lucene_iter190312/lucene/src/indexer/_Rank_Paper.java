/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import database.PaperPaperTB;
import database.RankPaperTB;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Date;

/**
 *
 * @author HuyDang
 */
public class _Rank_Paper {

    public _Rank_Paper() {
    }

    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            long start = new Date().getTime();
            int count = this._rank(connectionPool);
            long end = new Date().getTime();
            out = "Count: " + count + " record - Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    public int _rank(ConnectionPool connectionPool) {
        int count = 0;
        try {
            Connection connection = connectionPool.getConnection();
            Statement statement = connection.createStatement();
            // Drop
            String drop = "DROP TABLE IF EXISTS `" + RankPaperTB.TABLE_NAME + "`;";
            statement.execute(drop);
            // Create
            String create = "CREATE TABLE IF NOT EXISTS `" + RankPaperTB.TABLE_NAME + "` (`" + RankPaperTB.COLUMN_PAPERID + "` INT(10) NOT NULL, `" + RankPaperTB.COLUMN_CITATIONCOUNT + "` INT(10) NULL, `" + RankPaperTB.COLUMN_RANK + "` INT(10) NULL AUTO_INCREMENT, PRIMARY KEY (`" + RankPaperTB.COLUMN_PAPERID + "`), INDEX `index2` (`" + RankPaperTB.COLUMN_RANK + "` ASC)) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8;";
            statement.execute(create);
            // Clean up data
            String detele = "DELETE FROM " + PaperPaperTB.TABLE_NAME + " WHERE " + PaperPaperTB.COLUMN_PAPERID + " = " + PaperPaperTB.COLUMN_PAPERREFID + ";";
            statement.execute(detele);
            // Truncate all old ranking data
            String truncate = "TRUNCATE TABLE `" + RankPaperTB.TABLE_NAME + "`;";
            statement.execute(truncate);
            // -- Insert count data
            // 1. TABLE _rank_paper, citationCount: all paper cite to this paper
            String insert = "INSERT IGNORE INTO " + RankPaperTB.TABLE_NAME + "(" + RankPaperTB.COLUMN_PAPERID + ", " + RankPaperTB.COLUMN_CITATIONCOUNT + ") SELECT pp." + PaperPaperTB.COLUMN_PAPERREFID + ", COUNT(DISTINCT pp." + PaperPaperTB.COLUMN_PAPERID + ") FROM " + PaperPaperTB.TABLE_NAME + " pp GROUP BY pp." + PaperPaperTB.COLUMN_PAPERREFID + ";";
            count = statement.executeUpdate(insert);

            statement.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
        }
        return count;
    }

    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String user = "root";
            String pass = "@huydang1920@";
            String database = "cspublicationcrawler";
            int port = 3306;
            ConnectionPool connectionPool = new ConnectionPool(user, pass, database, port);
            _Rank_Paper indexer = new _Rank_Paper();
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}