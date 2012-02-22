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

    stmt.executeUpdate("INSERT INTO cboe_weeklies_ordered ( eod_date, ticker_symbol, close, adj_close ) SELECT eod_date, ticker_symbol, close, adj_close FROM cboe_weeklies_eod ORDER BY ticker_symbol ASC, eod_date ASC")

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

   stmt.executeUpdate("INSERT INTO cboe_weeklies_sharpe SELECT ticker_symbol, AVG(day_change), AVG(day_change)/STDDEV(day_change) FROM cboe_weeklies_returns WHERE eod_date>'01-JAN-11' GROUP BY ticker_symbol")

    conn.close()    
  } 
}