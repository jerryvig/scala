import java.io.FileWriter
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import com.gargoylesoftware.htmlunit._
import org.json._
import au.com.bytecode.opencsv._
import scala.collection.mutable._

object CrunchBaseStartups { 
  val driver = new HtmlUnitDriver( BrowserVersion.FIREFOX_3_6 )
  val writer = new CSVWriter( new FileWriter( "/tmp/CrunchBaseStartups.csv" ) )

  def main( args: Array[String] ) {
    driver.setJavascriptEnabled( false )
    
    getCompany()
    
    writer.close()
  }

  def getCompany() {
    driver.get( "http://api.crunchbase.com/v/1/company/pinterest.js" )
    val jsonObj = new JSONObject( driver.getPageSource() )

    val fundingArray = jsonObj.getJSONArray( "funding_rounds" )
    for ( i <- 0 until fundingArray.length() ) {
       val roundObject = fundingArray.getJSONObject(i)
      
       val fieldsArray = Array( roundObject.getString("raised_amount"), roundObject.getString("raised_currency_code"), roundObject.getString("funded_month"), roundObject.getString("funded_day") )
       writer.writeNext( fieldsArray )      
    }
  }

  def getCompanyList() {
    val letters = Array( "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" )
    for ( letter : String  <- letters ) {
      println( letter )
    } 
  }
}