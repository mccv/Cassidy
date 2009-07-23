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
    import scala.collection.jcl.Conversions._
    import org.scala_tools.javautils.Imports._

    private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None

    protected val client : Cassandra.Client

    val obtainedAt : Long
    
    def /(tableName : String, key : String, columnParent : String, start : Option[Int],end : Option[Int]) : List[column_t] = {
        client.get_slice(tableName, key, columnParent, start.getOrElse(-1),end.getOrElse(-1)).toList
    }
    
    def /(tableName : String, key : String, columnParent : String, colNames : List[String]) : List[column_t] = {
        client.get_slice_by_names(tableName, key, columnParent, colNames.asJava ).toList
    }

    def |(tableName : String, key : String, colPath : String) : Option[column_t] = {
        client.get_column(tableName, key, colPath)
    }

    def |#(tableName : String, key : String, columnParent : String) : Int = {
        client.get_column_count(tableName, key, columnParent)
    }

    def ++|(tableName : String, key : String, columnPath : String, cellData : Array[Byte], timestamp : Long, block : Boolean) = {
        client.insert(tableName, key, columnPath, cellData,timestamp,block)
    }

    def ++|(tableName : String, key : String, columnPath : String, cellData : Array[Byte], block : Boolean) = {
        client.insert(tableName,key,columnPath,cellData,obtainedAt,block)
    }

    def ++|(batch : batch_mutation_t, block : Boolean) = {
        client.batch_insert(batch, block)
    }

    def --(tableName : String, key : String, columnPathOrParent : String, timestamp : Long, block : Boolean) = {
        client.remove(tableName, key, columnPathOrParent, timestamp, block)
    }

    def --(tableName : String, key : String, columnPathOrParent : String, block : Boolean) = {
        client.remove(tableName, key, columnPathOrParent, obtainedAt, block)
    }

    def /@(tableName : String, key : String, columnParent : String, timestamp : Long) : List[column_t] = {
        client.get_columns_since(tableName, key, columnParent, timestamp).toList
    }

    def /^(tableName : String, key : String, columnFamily : String, start : Option[Int], end : Option[Int], count : Int ) : List[superColumn_t] = {
        client.get_slice_super(tableName, key,columnFamily, start.getOrElse(-1), end.getOrElse(-1)).toList //TODO upgrade thrift interface to support count
    }

    def /^(tableName : String, key : String, columnFamily : String, superColNames : List[String]) : List[superColumn_t] = {
        client.get_slice_super_by_names(tableName, key, columnFamily, superColNames.asJava).toList
    }

    def |^(tableName : String, key : String, superColumnPath : String) : Option[superColumn_t] = {
        client.get_superColumn(tableName,key,superColumnPath)
    }

    def ++|^ (batch : batch_mutation_super_t, block : Boolean) = {
        client.batch_insert_superColumn(batch, block)
    }

    def keys(tableName : String, startsWith : String, stopsAt : String, maxResults : Option[Int]) : List[String] = {
        client.get_key_range(tableName, startsWith, stopsAt, maxResults.getOrElse(-1)).toList
    }
    
    def property(name : String) : String = client.getStringProperty(name)
    def properties(name : String) : List[String] = client.getStringListProperty(name).toList
    def describeTable(tableName : String) = client.describeTable(tableName)

    def ?(query : String) = client.executeQuery(query)
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
            val obtainedAt = System.currentTimeMillis
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