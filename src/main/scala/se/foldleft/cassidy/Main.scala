/*
 * Main.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package se.foldleft.cassidy

import se.foldleft.pool._

object Main {

    

    def main(a : Array[String]) : Unit = {
        implicit def strToBytes(s : String) = s.getBytes("UTF-8")
        import scala.collection.jcl.Conversions._
     val c = new Cassidy(StackPool(SocketProvider("localhost",9160)),Protocol.Binary)
     c.doWork { s => {
                  val user_id = "1"
                  s.++|("users",user_id,"base_attributes:name", "Lord Foo Bar", false)
                  s.++|("users",user_id,"base_attributes:name", "24", false)

                  for( i <- s./("users", user_id, "base_attributes", None,None).toList) println(i)
                  

             }
     }

    }
}