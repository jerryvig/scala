import java.sql.{Connection, DriverManager, Statement}

object NormalizeCBOEWeekliesTT {
  def main( args: Array[String] ) {
    classOf[com.timesten.jdbc.TimesTenDriver]    
    val conn = DriverManager.getConnection("jdbc:timesten:direct:dsn=MORNINGSTAR")
    val stmt = conn.createStatement()

    try {
     stmt.executeUpdate("DROP SEQUENCE cboe_eod_seq")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE CBOE_WEEKLIES_ORDERED")
    }  catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE cboe_eod_seq INCREMENT BY 1")

    stmt.executeUpdate("CREATE TABLE CBOE_WEEKLIES_ORDERED ( idx INTEGER, eod_date DATE, ticker_symbol VARCHAR(10), adj_close DOUBLE PRECISION )")
    
    stmt.executeUpdate("INSERT INTO CBOE_WEEKLIES_ORDERED ( eod_date, ticker_symbol, adj_close ) SELECT eod_date, ticker_symbol, adj_close FROM cboe_weeklies_eod ORDER BY ticker_symbol ASC, eod_date ASC" )    

    stmt.executeUpdate("UPDATE CBOE_WEEKLIES_ORDERED set idx = cboe_eod_seq.nextval")

    stmt.executeUpdate("CREATE INDEX cboe_wo_id_idx ON CBOE_WEEKLIES_ORDERED ( idx )");
    stmt.executeUpdate("CREATE INDEX cboe_wo_ticker_idx ON CBOE_WEEKLIES_ORDERED ( ticker_symbol )")

    try {
      stmt.executeUpdate("DROP TABLE CBOE_WEEKLIES_RETURNS")
    } catch { case e:Exception => }
 
    stmt.executeUpdate("CREATE TABLE cboe_weeklies_returns ( idx INTEGER, eod_date DATE, ticker_symbol VARCHAR(10), adj_close DOUBLE PRECISION, day_change DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_weeklies_returns SELECT t1.idx, t1.eod_date, t1.ticker_symbol, t1.adj_close, (t1.adj_close-t2.adj_close)/t2.adj_close FROM cboe_weeklies_ordered t1, cboe_weeklies_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.idx=(t1.idx-1))")

    stmt.executeUpdate("CREATE INDEX cboe_wr_eod_idx ON cboe_weeklies_returns ( eod_date )")
    stmt.executeUpdate("CREATE INDEX cboe_wr_ts_idx ON cboe_weeklies_returns ( ticker_symbol )")
   
    try {
      stmt.executeUpdate("DROP TABLE cboe_weeklies_sharpe")
    } catch { case e:Exception => }
           
    stmt.executeUpdate("CREATE TABLE cboe_weeklies_sharpe ( ticker_symbol VARCHAR(10), avg_return DOUBLE PRECISION, sharpe_ratio DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_weeklies_sharpe SELECT ticker_symbol, AVG(day_change), AVG(day_change)/SQRT(SUM(day_change*day_change)/COUNT(*)-(SUM(day_change)/COUNT(*))*(SUM(day_change)/COUNT(*))) FROM cboe_weeklies_returns GROUP BY ticker_symbol")

    try {
      stmt.executeUpdate("DROP SEQUENCE cboe_weekly_revs_seq")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE cboe_weeklies_revs_ordered")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE cboe_weekly_revs_seq INCREMENT BY 1")

    stmt.executeUpdate("CREATE TABLE cboe_weeklies_revs_ordered ( idx INTEGER, ticker_symbol VARCHAR(10), period VARCHAR(16), revenue DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_weeklies_revs_ordered ( ticker_symbol, period, revenue ) SELECT * FROM CBOE_WEEKLIES_REVENUE ORDER BY ticker_symbol ASC, period ASC")

    stmt.executeUpdate("UPDATE cboe_weeklies_revs_ordered set idx=cboe_weekly_revs_seq.nextval")

    try {
      stmt.executeUpdate("DROP TABLE cboe_revenue_growth")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE cboe_revenue_growth ( idx INTEGER, ticker_symbol VARCHAR(10), period VARCHAR(16), revenue DOUBLE PRECISION, rev_growth DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_revenue_growth SELECT t1.idx, t1.ticker_symbol, t1.period, t1.revenue, (t1.revenue-t2.revenue)/t2.revenue FROM cboe_weeklies_revs_ordered t1, cboe_weeklies_revs_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.idx=(t1.idx-1)AND t2.revenue>0)")

    try {
      stmt.executeUpdate("DROP TABLE cboe_sharpe_revg")
    } catch { case e:Exception => }
    
    stmt.executeUpdate("CREATE TABLE cboe_sharpe_revg ( ticker_symbol VARCHAR(10), avg_rev_growth DOUBLE PRECISION, sharpe_rev_growth DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_sharpe_revg SELECT ticker_symbol, AVG(rev_growth), AVG(rev_growth)/SQRT(SUM(rev_growth*rev_growth)/COUNT(*)-(SUM(rev_growth)/COUNT(rev_growth))*(SUM(rev_growth)/COUNT(*))) FROM cboe_revenue_growth GROUP BY ticker_symbol")

    conn.close()    
  } 
}