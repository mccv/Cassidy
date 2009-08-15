/*
 * Main.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package se.foldleft.cassidy

import org.apache.cassandra.service.{ConsistencyLevel,ColumnPath}
import se.foldleft.pool._

object Main {

    

  /*
  import se.foldleft.pool._
  import se.foldleft.cassidy._
  import org.apache.cassandra.service._
   */

    def main(a : Array[String]) : Unit = {
        implicit def strToBytes(s : String) = s.getBytes("UTF-8")
        import scala.collection.jcl.Conversions._
     val c = new Cassidy(StackPool(SocketProvider("localhost",9160)),Protocol.Binary,ConsistencyLevel.QUORUM)

     c.doWork { s => { s++|("SocialGraph","mccv",new ColumnPath("Details",null,"name"),"Mark McBride")}}

    }
}