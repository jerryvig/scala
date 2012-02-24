import java.sql.{Connection, DriverManager, Statement, ResultSet}

object NormalizeCBOEWeeklies {
  def main( args: Array[String] ) {
    classOf[oracle.jdbc.OracleDriver]
    val conn = DriverManager.getConnection("jdbc:oracle:thin:morningstar/uptime5@localhost:1521:XE")

    val stmt = conn.createStatement()
    try {
     stmt.executeUpdate("DROP SEQUENCE cboe_weekly_returns_seq")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE cboe_weeklies_ordered")
    }  catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE cboe_weekly_returns_seq START WITH 1 INCREMENT BY 1")

    stmt.executeUpdate("CREATE TABLE cboe_weeklies_ordered ( idx NUMBER PRIMARY KEY, eod_date DATE, ticker_symbol VARCHAR(10), close DOUBLE PRECISION, adj_close DOUBLE PRECISION )")
    
    stmt.executeUpdate("CREATE OR REPLACE TRIGGER cboe_wo_trigger BEFORE INSERT ON cboe_weeklies_ordered REFERENCING NEW AS NEW FOR EACH ROW BEGIN SELECT cboe_weekly_returns_seq.nextval INTO :NEW.IDX FROM dual; END;")

    stmt.executeUpdate("INSERT INTO cboe_weeklies_ordered ( eod_date, ticker_symbol, close, adj_close ) SELECT eod_date, ticker_symbol, close, adj_close FROM cboe_weeklies_eod WHERE eod_date>'01-JAN-11' ORDER BY ticker_symbol ASC, eod_date ASC")

     try {
      stmt.executeUpdate("DROP TABLE cboe_weeklies_returns")
    }  catch { case e:Exception => }
  
    stmt.executeUpdate("CREATE TABLE cboe_weeklies_returns ( idx NUMBER PRIMARY KEY, eod_date DATE, ticker_symbol VARCHAR(10), close DOUBLE PRECISION, adj_close DOUBLE PRECISION, day_change DOUBLE PRECISION )")
   
    stmt.executeUpdate("INSERT INTO cboe_weeklies_returns SELECT t1.idx, t1.eod_date, t1.ticker_symbol, t1.close, t1.adj_close, (t1.adj_close-t2.adj_close)/t2.adj_close FROM cboe_weeklies_ordered t1, cboe_weeklies_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.idx=(t1.idx-1)) ORDER BY t1.idx ASC" )
 
   stmt.executeUpdate("CREATE INDEX cboe_wr_eod_idx ON cboe_weeklies_returns ( eod_date )")
   stmt.executeUpdate("CREATE INDEX cboe_wr_ts_idx ON cboe_weeklies_returns ( ticker_symbol )")

   try {
      stmt.executeUpdate("DROP TABLE cboe_weeklies_sharpe")
   }  catch { case e:Exception => }

   stmt.executeUpdate("CREATE TABLE cboe_weeklies_sharpe ( ticker_symbol VARCHAR(10), avg_return DOUBLE PRECISION, sharpe_ratio DOUBLE PRECISION )")

   stmt.executeUpdate("INSERT INTO cboe_weeklies_sharpe SELECT ticker_symbol, AVG(day_change), AVG(day_change)/STDDEV_POP(day_change) FROM cboe_weeklies_returns GROUP BY ticker_symbol")

    //For revenue growth data from Morningstar
    try {
      stmt.executeUpdate("DROP SEQUENCE cboe_weekly_revs_seq")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE cboe_weeklies_revs_ordered")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE cboe_weekly_revs_seq START WITH 1 INCREMENT BY 1")
    
    stmt.executeUpdate("CREATE TABLE cboe_weeklies_revs_ordered ( idx NUMBER PRIMARY KEY, ticker_symbol VARCHAR(10), period VARCHAR(16), revenue DOUBLE PRECISION )")

    stmt.executeUpdate("CREATE OR REPLACE TRIGGER cboe_rev_trigger BEFORE INSERT ON cboe_weeklies_revs_ordered REFERENCING NEW AS NEW FOR EACH ROW BEGIN SELECT cboe_weekly_revs_seq.nextval INTO :NEW.IDX FROM dual; END;")

    stmt.executeUpdate("INSERT INTO cboe_weeklies_revs_ordered ( ticker_symbol, period, revenue ) SELECT * FROM CBOE_WEEKLIES_REVENUE ORDER BY ticker_symbol ASC, period ASC")

    try {
      stmt.executeUpdate("DROP TABLE cboe_revenue_growth")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE cboe_revenue_growth ( idx NUMBER, ticker_symbol VARCHAR(10), period VARCHAR(16), revenue DOUBLE PRECISION, rev_growth DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_revenue_growth SELECT t1.idx, t1.ticker_symbol, t1.period, t1.revenue, (t1.revenue-t2.revenue)/t2.revenue FROM cboe_weeklies_revs_ordered t1, cboe_weeklies_revs_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.idx=(t1.idx-1) AND t2.revenue>0) ORDER BY t1.idx ASC")

    try {
      stmt.executeUpdate("DROP TABLE cboe_sharpe_revg");
    } catch { case e:Exception => }
    
    stmt.executeUpdate("CREATE TABLE cboe_sharpe_revg ( ticker_symbol VARCHAR(10), avg_rev_growth DOUBLE PRECISION, sharpe_rev_growth DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_sharpe_revg SELECT ticker_symbol, AVG(rev_growth), AVG(rev_growth)/STDDEV_POP(rev_growth) FROM cboe_revenue_growth GROUP BY ticker_symbol")

    try {
      stmt.executeUpdate("DROP SEQUENCE rank_seq");
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE rank_seq START WITH 1 INCREMENT BY 1");
   
    try {
     stmt.executeUpdate("DROP TABLE sharpe_ratio_ranks");
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE sharpe_ratio_ranks ( idx INTEGER, ticker_symbol VARCHAR(10) )")

    try {
      stmt.executeUpdate("DROP TRIGGER sharpe_ranks_trigger")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE OR REPLACE TRIGGER sharpe_ranks_trigger BEFORE INSERT ON sharpe_ratio_ranks REFERENCING NEW AS NEW FOR EACH ROW BEGIN SELECT rank_seq.nextval INTO :NEW.IDX FROM dual; END;")

    stmt.executeUpdate("INSERT INTO sharpe_ratio_ranks (ticker_symbol) SELECT ticker_symbol FROM cboe_weeklies_sharpe ORDER BY sharpe_ratio DESC");

    try { 
     stmt.executeUpdate("DROP SEQUENCE rank_seq")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE rank_seq START WITH 1 INCREMENT BY 1");

    try {
     stmt.executeUpdate("DROP TABLE sharpe_revg_ranks")
     } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE sharpe_revg_ranks ( idx INTEGER, ticker_symbol VARCHAR(10) )")

    try {
      stmt.executeUpdate("DROP TRIGGER sharpe_ranks_trigger")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE OR REPLACE TRIGGER sharpe_ranks_trigger BEFORE INSERT ON sharpe_revg_ranks REFERENCING NEW AS NEW FOR EACH ROW BEGIN SELECT rank_seq.nextval INTO :NEW.IDX FROM dual; END;");

    stmt.executeUpdate("INSERT INTO sharpe_revg_ranks (ticker_symbol) SELECT ticker_symbol FROM cboe_sharpe_revg ORDER BY sharpe_rev_growth DESC")

    try {
      stmt.executeUpdate("DROP TABLE cboe_norm_ranks")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE cboe_norm_ranks ( ticker_symbol VARCHAR(10), norm_rank DOUBLE PRECISION )");

    stmt.executeUpdate("INSERT INTO cboe_norm_ranks SELECT t1.ticker_symbol, SQRT(t1.idx*t1.idx+t2.idx*t2.idx) AS rank_norm FROM sharpe_ratio_ranks t1, sharpe_revg_ranks t2 WHERE (t2.ticker_symbol=t1.ticker_symbol) order by rank_norm ASC")

    try {
     stmt.executeUpdate("DROP SEQUENCE rank_seq")
    } catch { case e:Exception => }

    conn.close()    
  } 
}