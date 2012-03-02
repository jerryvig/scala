import java.sql.{Connection, DriverManager, Statement}

object NormalizeCBOEWeekliesTT {
  def main( args: Array[String] ) {
    classOf[com.timesten.jdbc.TimesTenDriver]    
    val conn = DriverManager.getConnection("jdbc:timesten:direct:dsn=MORNINGSTAR")
    val stmt = conn.createStatement()

    try {
     stmt.executeUpdate("DROP TABLE price_to_deps_ranks")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE sharpe_deps_ranks")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE sharpe_nm_ranks")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE sharpe_fcf_ranks")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE price_to_deps")
    } catch { case e:Exception => }

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

    try {
     stmt.executeUpdate("DROP TABLE cboe_sharpe_ratios")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE cboe_sharpe_ratios ( ticker_symbol VARCHAR(10), sharpe_ratio DOUBLE PRECISION, sharpe_rev_growth DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_sharpe_ratios SELECT t1.ticker_symbol, t1.sharpe_ratio, t2.sharpe_rev_growth FROM cboe_weeklies_sharpe t1, cboe_sharpe_revg t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t1.sharpe_ratio>0.0 AND t2.sharpe_rev_growth>0.0)")

    try {
      stmt.executeUpdate("DROP SEQUENCE rank_seq")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE rank_seq INCREMENT BY 1")

    try {
      stmt.executeUpdate("DROP TABLE sharpe_ratio_ranks")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE sharpe_ratio_ranks ( idx INTEGER, ticker_symbol VARCHAR(10) )")

    stmt.executeUpdate("INSERT INTO sharpe_ratio_ranks (ticker_symbol) SELECT ticker_symbol FROM cboe_sharpe_ratios ORDER BY sharpe_ratio DESC")
  
    stmt.executeUpdate("UPDATE sharpe_ratio_ranks SET idx=rank_seq.nextval")

    try {
      stmt.executeUpdate("DROP SEQUENCE rank_seq")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE rank_seq INCREMENT BY 1")

    try {
     stmt.executeUpdate("DROP TABLE sharpe_revg_ranks")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE sharpe_revg_ranks ( idx INTEGER, ticker_symbol VARCHAR(10) )")

    stmt.executeUpdate("INSERT INTO sharpe_revg_ranks (ticker_symbol) SELECT ticker_symbol FROM cboe_sharpe_ratios ORDER BY sharpe_rev_growth DESC")

    stmt.executeUpdate("UPDATE sharpe_revg_ranks SET idx=rank_seq.nextval")

    try {
      stmt.executeUpdate("DROP SEQUENCE cboe_fcf_seq")
    } catch { case e:Exception => }
    
    try {
      stmt.executeUpdate("DROP TABLE cboe_fcf_ordered")   
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE SEQUENCE cboe_fcf_seq INCREMENT BY 1")

    stmt.executeUpdate("CREATE TABLE cboe_fcf_ordered ( idx INTEGER, ticker_symbol VARCHAR(10), period VARCHAR(16), free_cash_flow DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_fcf_ordered ( ticker_symbol, period, free_cash_flow ) SELECT * FROM cboe_weeklies_fcf ORDER BY ticker_symbol ASC, period ASC")

    stmt.executeUpdate("UPDATE cboe_fcf_ordered set idx=cboe_fcf_seq.nextval")

    try {
      stmt.executeUpdate("DROP TABLE cboe_fcf_growth")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE cboe_fcf_growth ( idx INTEGER, ticker_symbol VARCHAR(10), period VARCHAR(16), free_cash_flow DOUBLE PRECISION, fcf_growth DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_fcf_growth SELECT t1.idx, t1.ticker_symbol, t1.period, t1.free_cash_flow, (t1.free_cash_flow-t2.free_cash_flow)/t2.free_cash_flow FROM cboe_fcf_ordered t1, cboe_fcf_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.idx=(t1.idx-1) AND t2.free_cash_flow>0)")

    try {
      stmt.executeUpdate("DROP TABLE cboe_sharpe_fcfg")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE cboe_sharpe_fcfg ( ticker_symbol VARCHAR(10), avg_fcfg DOUBLE PRECISION, sharpe_fcfg DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_sharpe_fcfg SELECT ticker_symbol, AVG(fcf_growth), AVG(fcf_growth)/SQRT(AVG(fcf_growth*fcf_growth)-AVG(fcf_growth)*AVG(fcf_growth)) FROM cboe_fcf_growth WHERE period!='TTM' GROUP BY ticker_symbol HAVING COUNT(*)>1");

    try {
      stmt.executeUpdate("DROP SEQUENCE cboe_nm_seq")
    } catch { case e:Exception => }

    try {
      stmt.executeUpdate("DROP TABLE cboe_nm_ordered")
    } catch { case e:Exception => }
   
    stmt.executeUpdate("CREATE SEQUENCE cboe_nm_seq INCREMENT BY 1")

    stmt.executeUpdate("CREATE TABLE cboe_nm_ordered ( idx INTEGER, ticker_symbol VARCHAR(10), period VARCHAR(16), net_margin DOUBLE PRECISION )")
    
    stmt.executeUpdate("INSERT INTO cboe_nm_ordered ( ticker_symbol, period, net_margin ) SELECT * FROM cboe_weeklies_net_margin ORDER BY ticker_symbol ASC, period ASC")

    stmt.executeUpdate("UPDATE cboe_nm_ordered SET idx=cboe_nm_seq.nextval")

    try {
      stmt.executeUpdate("DROP TABLE cboe_nm_growth")
    } catch { case e:Exception => }
   
    stmt.executeUpdate("CREATE TABLE cboe_nm_growth ( idx INTEGER, ticker_symbol VARCHAR(10), period VARCHAR(16), net_margin DOUBLE PRECISION, nm_growth DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_nm_growth SELECT t1.idx, t1.ticker_symbol, t1.period, t1.net_margin, (t1.net_margin-t2.net_margin)/t2.net_margin FROM cboe_nm_ordered t1, cboe_nm_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.idx=(t1.idx-1) AND t2.net_margin>0)")

    try {
      stmt.executeUpdate("DROP TABLE cboe_sharpe_nmg")
    } catch { case e:Exception => }

    stmt.executeUpdate("CREATE TABLE cboe_sharpe_nmg ( ticker_symbol VARCHAR(10), avg_nmg DOUBLE PRECISION, sharpe_nmg DOUBLE PRECISION )")

    stmt.executeUpdate("INSERT INTO cboe_sharpe_nmg SELECT ticker_symbol, AVG(nm_growth), AVG(nm_growth)/SQRT(AVG(nm_growth*nm_growth)-AVG(nm_growth)*AVG(nm_growth)) FROM cboe_nm_growth WHERE period!='TTM' GROUP BY ticker_symbol HAVING COUNT(*)>1")

   try {
     stmt.executeUpdate("DROP SEQUENCE cboe_eps_seq") 
   } catch { case e:Exception => }

   try {
     stmt.executeUpdate("DROP TABLE cboe_deps_ordered")
   } catch { case e:Exception => }

   stmt.executeUpdate("CREATE SEQUENCE cboe_eps_seq INCREMENT BY 1")

   stmt.executeUpdate("CREATE TABLE cboe_deps_ordered ( idx INTEGER, ticker_symbol VARCHAR(10), period VARCHAR(16), diluted_eps DOUBLE PRECISION )")

   stmt.executeUpdate("INSERT INTO cboe_deps_ordered ( ticker_symbol, period, diluted_eps ) SELECT ticker_symbol, period, diluted_eps FROM cboe_weeklies_eps ORDER BY ticker_symbol ASC, period ASC")

   stmt.executeUpdate("UPDATE cboe_deps_ordered SET idx=cboe_eps_seq.nextval")

   try {
     stmt.executeUpdate("DROP TABLE cboe_deps_growth")
   } catch { case e:Exception => }

   stmt.executeUpdate("CREATE TABLE cboe_deps_growth ( idx INTEGER, ticker_symbol VARCHAR(10), period VARCHAR(16), dilted_eps DOUBLE PRECISION, deps_growth DOUBLE PRECISION )")

   stmt.executeUpdate("INSERT INTO cboe_deps_growth SELECT t1.idx, t1.ticker_symbol, t1.period, t1.diluted_eps, (t1.diluted_eps-t2.diluted_eps)/t2.diluted_eps FROM cboe_deps_ordered t1, cboe_deps_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.idx=(t1.idx-1) AND t2.diluted_eps>0)")

   try {
     stmt.executeUpdate("DROP TABLE cboe_sharpe_depsg")
   } catch { case e:Exception => }

   stmt.executeUpdate("CREATE TABLE cboe_sharpe_depsg ( ticker_symbol VARCHAR(10), avg_depsg DOUBLE PRECISION, sharpe_depsg DOUBLE PRECISION )")

   stmt.executeUpdate("INSERT INTO cboe_sharpe_depsg SELECT ticker_symbol, AVG(deps_growth), AVG(deps_growth)/SQRT(AVG(deps_growth*deps_growth)-AVG(deps_growth)*AVG(deps_growth)) FROM cboe_deps_growth WHERE period!='TTM' GROUP BY ticker_symbol HAVING COUNT(*)>1")

   stmt.executeUpdate("CREATE TABLE price_to_deps ( ticker_symbol VARCHAR(10), price_to_deps DOUBLE PRECISION )")

   stmt.executeUpdate("INSERT INTO price_to_deps SELECT t1.ticker_symbol, t1.close/t2.diluted_eps FROM cboe_weeklies_eod t1, cboe_weeklies_eps t2 WHERE ((t1.eod_date='2012-02-29') AND t2.ticker_symbol=t1.ticker_symbol AND t2.period='TTM' AND t2.diluted_eps>0)")

   try {
     stmt.executeUpdate("DROP SEQUENCE rank_seq")
   } catch { case e:Exception => }

   stmt.executeUpdate("CREATE SEQUENCE rank_seq INCREMENT BY 1")

   stmt.executeUpdate("CREATE TABLE sharpe_fcf_ranks ( idx INTEGER, ticker_symbol VARCHAR(10) )")

   stmt.executeUpdate("INSERT INTO sharpe_fcf_ranks (ticker_symbol) SELECT ticker_symbol FROM cboe_sharpe_fcfg ORDER BY sharpe_fcfg DESC")

   stmt.executeUpdate("UPDATE sharpe_fcf_ranks SET idx=rank_seq.nextval")

   try {
     stmt.executeUpdate("DROP SEQUENCE rank_seq")
   } catch { case e:Exception => }

   stmt.executeUpdate("CREATE SEQUENCE rank_seq INCREMENT BY 1")

   stmt.executeUpdate("CREATE TABLE sharpe_nm_ranks ( idx INTEGER, ticker_symbol VARCHAR(10) )")

   stmt.executeUpdate("INSERT INTO sharpe_nm_ranks (ticker_symbol) SELECT ticker_symbol FROM cboe_sharpe_nmg ORDER BY sharpe_nmg DESC")

   stmt.executeUpdate("UPDATE sharpe_nm_ranks SET idx=rank_seq.nextval")

   try {
     stmt.executeUpdate("DROP SEQUENCE rank_seq")
   } catch { case e:Exception => }

   stmt.executeUpdate("CREATE SEQUENCE rank_seq INCREMENT BY 1")

   stmt.executeUpdate("CREATE TABLE sharpe_deps_ranks ( idx INTEGER, ticker_symbol VARCHAR(10) )")

   stmt.executeUpdate("INSERT INTO sharpe_deps_ranks (ticker_symbol) SELECT ticker_symbol FROM cboe_sharpe_depsg ORDER BY sharpe_depsg DESC")

   stmt.executeUpdate("UPDATE sharpe_deps_ranks SET idx=rank_seq.nextval")

   try {
     stmt.executeUpdate("DROP SEQUENCE rank_seq")
   } catch { case e:Exception => }

   stmt.executeUpdate("CREATE SEQUENCE rank_seq INCREMENT BY 1")
  
   stmt.executeUpdate("CREATE TABLE price_to_deps_ranks ( idx INTEGER, ticker_symbol VARCHAR(10) )")

   stmt.executeUpdate("INSERT INTO price_to_deps_ranks (ticker_symbol) SELECT ticker_symbol FROM price_to_deps ORDER BY price_to_deps ASC")

   stmt.executeUpdate("UPDATE price_to_deps_ranks SET idx=rank_seq.nextval")

   try {
      stmt.executeUpdate("DROP TABLE cboe_norm_ranks")
   } catch { case e:Exception => }

   stmt.executeUpdate("CREATE TABLE cboe_norm_ranks ( ticker_symbol VARCHAR(10), sharpe_ratio_rank INTEGER, sharpe_revg_rank INTEGER, sharpe_fcfg_rank INTEGER, price_to_deps_rank INTEGER, norm_rank DOUBLE PRECISION )")

   stmt.executeUpdate("INSERT INTO cboe_norm_ranks SELECT t1.ticker_symbol, t1.idx, t2.idx, t3.idx, t6.idx, SQRT(t1.idx*t1.idx + t2.idx*t2.idx + t3.idx*t3.idx + t6.idx*t6.idx) FROM sharpe_ratio_ranks t1, sharpe_revg_ranks t2, sharpe_fcf_ranks t3, price_to_deps_ranks t6 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t3.ticker_symbol=t1.ticker_symbol AND t6.ticker_symbol=t1.ticker_symbol)")

   conn.close()
  }
}