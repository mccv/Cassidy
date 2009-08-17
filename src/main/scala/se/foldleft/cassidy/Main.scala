/*
 * Main.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package se.foldleft.cassidy

import org.apache.cassandra.service.{ConsistencyLevel, ColumnPath}
import se.foldleft.pool._

object Main {



  def main(a: Array[String]): Unit = {
    work()
  }

  def work() = {
    implicit def strToBytes(s: String) = s.getBytes("UTF-8")
    import scala.collection.jcl.Conversions._
    val c = new Cassidy(StackPool(SocketProvider("localhost", 9160)), Protocol.Binary, ConsistencyLevel.QUORUM)

    /*
     Note: the keyspace def to use this looks like
        <Keyspace Name="Delicious">
            <KeysCachedFraction>0.01</KeysCachedFraction>
            <ColumnFamily CompareWith="UTF8Type" Name="Users"/>
      	    <ColumnFamily CompareWith="UTF8Type" Name="Tags"/>
            <ColumnFamily CompareWith="UTF8Type" Name="Bookmarks"/>
            <ColumnFamily ColumnType="Super" CompareWith="UTF8Type" CompareSubcolumnsWith="UTF8Type" Name="UserBookmarks"/>
            <ColumnFamily ColumnType="Super" CompareWith="UTF8Type" CompareSubcolumnsWith="UTF8Type" Name="UserTags"/>
        </Keyspace>
 
     */
    c.doWork {
      s => {
        println("exercising inserts")
        s ++| ("Delicious", "mccv", new ColumnPath("Users", null, "name"), "Mark McBride")
        s / ("Delicious") ++| ("mccv", new ColumnPath("Users", null, "location"), "Santa Clara")
        s / ("Delicious") / ("mccv") ++| (new ColumnPath("Users", null, "state"), "CA")
        s/"Delicious"/"mccv"/"Users" ++| ("age","34-ish")
        s/"Delicious"/"mccv"/"Users"/"weight" ++| "too much"

        // now get all the values back
        println("exercising reads")
        val cf = s/"Delicious"/"mccv"/"Users"
        println("users/mccv has " + (cf|#) + " columns")
        cf/"name"| match {
          case Some(col) => println("mccv's name is " + new String(col.value))
          case None => println("no name found for mccv")
        }

        // play with super columns
        s/"Delicious"/"mccv"/^"UserBookmarks"/"http://www.twitter.com"/"description" ++| "the twitter page"

        // read out super columns
        val scf = s/"Delicious"/"mccv"/^"UserBookmarks"
        println((scf|#) + " bookmarks for mccv")
        scf/"http://www.twitter.com"/"description"| match {
          case Some(col) => println("desc for twitter page is " + new String(col.value))
          case None => println("no desc for this bookmark")

        }
      }
    }
  }
}