package se.foldleft.cassidy

import org.apache.cassandra.service._
import org.apache.thrift._
import org.apache.thrift.transport._
import org.apache.thrift.protocol._
import java.io.{Flushable,Closeable}
import se.foldleft.pool._

/**
 * Session represents - guess what - a Session!
 */
trait Session extends Closeable with Flushable
{
    val client : Cassandra.Client
}

/**
 * Cassidy is a wrapper around Cassandra that uses the Thrift java bindings with a TTransport pool
 * You can find TransportPool samples in PoolBridge.scala
 */
class Cassidy[T <: TTransport](transportPool : Pool[T], inputProtocol : Protocol, outputProtocol : Protocol) extends Closeable
{
    def this(transportPool : Pool[T], ioProtocol : Protocol) = this(transportPool,ioProtocol,ioProtocol)
    
    def newSession : Session = {
        val t = transportPool.borrowObject
        new Session
        {
            val client = new Cassandra.Client(inputProtocol.factory.getProtocol(t),outputProtocol.factory.getProtocol(t))
            def flush = t.flush
            def close = transportPool.returnObject(t)
        }
    }

    def doWork[R](work : (Session) => R) = {
        val s = newSession
        try
        {
           work(s)
           s.flush
        }
        finally
        {
            s.close
        }
    }

    def close = transportPool.close
}

/**
 * Protocol is just a simple wrapper over TProtocolFactory
 */
abstract class Protocol(val factory : TProtocolFactory)

object Protocol
{
    object Binary extends Protocol(new TBinaryProtocol.Factory)
    object SimpleJSON extends Protocol(new TSimpleJSONProtocol.Factory)
    object JSON extends Protocol(new TJSONProtocol.Factory)
}

object Main {
    def main(a : Array[String]) : Unit = {
        val c = new Cassidy(StackPool(SocketProvider("localhost",9610)),Protocol.Binary)

        c.doWork { x => println(x) }
    }
}
