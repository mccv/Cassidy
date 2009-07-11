/**
 * Welcome to Cassidy, the Scala Cassandra client
 */

package se.foldleft.cassidy

import org.apache.cassandra.service._
import org.apache.thrift._
import org.apache.thrift.transport._
import org.apache.thrift.protocol._
import java.io.{Flushable,Closeable}
import se.foldleft.pool._

trait Session extends Closeable with Flushable
{
    val client : Cassandra.Client
}

class Cassidy[T <: TTransport](transportPool : Pool[T], inputProtocol : Protocol, outputProtocol : Protocol) extends Closeable
{
    def this(transportPool : Pool[T], ioProtocol : Protocol) = this(transportPool,ioProtocol,ioProtocol)
    
    def newSession : Session = {
        val t = transportPool.borrowObject

        val c = new Cassandra.Client(inputProtocol(t),outputProtocol(t))

        new Session
        {
            val client = c
            def flush = t.flush
            def close = transportPool.returnObject(t)
        }
    }

    def doWork[R](work : (Session) => R) = {
        val s = newSession
        try
        {
            val r = work(s)
            s.flush

            r
        }
        finally
        {
            s.close
        }
    }

    def close = transportPool.close
}

sealed abstract class Protocol(val factory : TProtocolFactory)
{
    def apply(transport : TTransport) = factory.getProtocol(transport)
}

object Protocol
{
    object Binary extends Protocol(new TBinaryProtocol.Factory)
    object SimpleJSON extends Protocol(new TSimpleJSONProtocol.Factory)
    object JSON extends Protocol(new TJSONProtocol.Factory)
}
