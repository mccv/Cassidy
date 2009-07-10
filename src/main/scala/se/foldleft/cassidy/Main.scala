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
        val c = new Cassidy(StackPool(SocketProvider("localhost",9610)),Protocol.Binary)

        c.doWork { case s : Session => println(s) }
    }
}
