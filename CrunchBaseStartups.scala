import java.io._
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import com.gargoylesoftware.htmlunit._
import org.json._
import au.com.bytecode.opencsv._
import scala.collection.mutable._
import org.openqa.selenium._

object CrunchBaseStartups { 
  val driver = new HtmlUnitDriver( BrowserVersion.FIREFOX_3_6 )
  val writer = new CSVWriter( new FileWriter( "/tmp/CrunchBaseStartups.csv" ) )

  def main( args: Array[String] ) {
    driver.setJavascriptEnabled( false )    
    getJSONRecords()
    writer.close()
  }

  def getCompany( urlString : String ) {
    driver.get( urlString )
    val jsonObj = new JSONObject( driver.getPageSource() )

    try {
     val companyName = jsonObj.getString("name")
     val crunchBaseUrl = jsonObj.getString("crunchbase_url")
     val companyUrl = jsonObj.getString("homepage_url")
     val fundingArray = jsonObj.getJSONArray( "funding_rounds" )
     val totalRaised = jsonObj.getString("total_money_raised")
     val numberOfEmployees = jsonObj.getString("number_of_employees")
     val officesArray = jsonObj.getJSONArray("offices")
     var city = ""
     var stateCode = ""
     var countryCode = ""
     if ( officesArray.length() > 0 ) {
      val officesObj = officesArray.getJSONObject(0)
      city = officesObj.getString("city")
      stateCode = officesObj.getString("state_code")
      countryCode = officesObj.getString("country_code")
     }
     println( companyName + ", " + crunchBaseUrl + ", " + city )

     for ( i <- 0 until fundingArray.length() ) {
       val roundObject = fundingArray.getJSONObject(i)      
       writer.writeNext( Array( companyName, city, stateCode, countryCode, crunchBaseUrl, companyUrl, totalRaised, roundObject.getString("raised_amount"), roundObject.getString("raised_currency_code"), roundObject.getString("funded_year"), roundObject.getString("funded_month"), roundObject.getString("funded_day") ) )
     }
    } catch { case e:Exception => e.printStackTrace }   
  }

  def getCompanyList() {
    val letters = Array( "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" )
    val listWriter = new BufferedWriter( new FileWriter("/tmp/CrunchBaseList.csv") )

    for ( letter : String  <- letters ) {
      println( "GETing " + letter )
      driver.get( "http://www.crunchbase.com/companies?c="+letter )
      val col2Table = driver.findElementByXPath("//table[@class='col2_table_listing']")
      val liList = col2Table.findElements( By.tagName("li") )
      for ( i <- 0 until liList.size() ) {
        val companyName = liList.get(i).getText().trim()
        val anchor = liList.get(i).findElement( By.tagName("a") )
        val href = anchor.getAttribute("href").trim()
        listWriter.write( "\"" + companyName + "\",\"" + href + "\"\n" )  
      }
    }   
    listWriter.close() 
  }

  def getJSONUrls() {
    val lines = scala.io.Source.fromFile("/tmp/CrunchBaseList.csv").getLines()
    val jsonUrlsWriter = new BufferedWriter( new FileWriter("/tmp/CrunchBaseJSONUrls.csv") )   
 
    for ( line : String <- lines ) {
       val cols = line.split(",")
       for ( i <- 0 until cols.length )  
         cols(i) = cols(i).replaceAll("\"","")
       
       try {
        driver.get( cols(1) )
        val col3Div = driver.findElementByXPath("//div[@id='col3']")
        val aList = col3Div.findElements( By.tagName("a") )
        for ( i <- 0 until aList.size() ) {
         if ( aList.get(i).getText().contains("JSON") ) {
           val jsonUrl = aList.get(i).getAttribute("href").trim()
           jsonUrlsWriter.write( "\"" + cols(0) + "\",\"" + cols(1) + "\",\"" + jsonUrl +"\"\n" ) 
         }
        }
       } catch { case e:Exception => e.printStackTrace }   
    }
    jsonUrlsWriter.close()
  }

  def getJSONRecords() {
    val rows = (new CSVReader( new FileReader("/tmp/CrunchBaseJSONUrls.csv") )).readAll()
    
    for ( i <- 0 until rows.size() ) {
       val cols = rows.get(i)
       getCompany( cols(2) )
       Thread.sleep( 300 );
    }
  }
}