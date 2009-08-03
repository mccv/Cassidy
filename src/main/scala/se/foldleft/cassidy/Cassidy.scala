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
    val consistencyLevel : Int

    def /(keyspace : String, key : String, columnParent : ColumnParent, start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int) : List[Column] =
        /(keyspace,key,columnParent,start,end,ascending,count,consistencyLevel)

    def /(keyspace : String, key : String, columnParent : ColumnParent, start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int, consistencyLevel : Int) : List[Column] =
        client.get_slice(keyspace, key, columnParent, start, end, ascending, count, consistencyLevel).toList

    def /(keyspace : String, key : String, columnParent : ColumnParent, colNames : List[Array[Byte]]) : List[Column] =
        /(keyspace,key,columnParent,colNames,consistencyLevel)

    def /(keyspace : String, key : String, columnParent : ColumnParent, colNames : List[Array[Byte]], consistencyLevel : Int) : List[Column] =
        client.get_slice_by_names(keyspace, key, columnParent, colNames.asJava, consistencyLevel ).toList

    def |(keyspace : String, key : String, colPath : ColumnPath) : Option[Column] =
        |(keyspace,key,colPath,consistencyLevel)

    def |(keyspace : String, key : String, colPath : ColumnPath, consistencyLevel : Int) : Option[Column] =
        client.get_column(keyspace, key, colPath, consistencyLevel)

    def |#(keyspace : String, key : String, columnParent : ColumnParent) : Int =
        |#(keyspace,key,columnParent,consistencyLevel)

    def |#(keyspace : String, key : String, columnParent : ColumnParent, consistencyLevel : Int) : Int =
        client.get_column_count(keyspace, key, columnParent, consistencyLevel)

    def ++|(keyspace : String, key : String, columnPath : ColumnPath, value : Array[Byte]) : Unit =
        ++|(keyspace,key,columnPath,value,obtainedAt,consistencyLevel)

    def ++|(keyspace : String, key : String, columnPath : ColumnPath, value : Array[Byte], timestamp : Long) : Unit =
        ++|(keyspace,key,columnPath,value,timestamp,consistencyLevel)

    def ++|(keyspace : String, key : String, columnPath : ColumnPath, value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
        client.insert(keyspace, key, columnPath, value,timestamp,consistencyLevel)

    def ++|(keyspace : String, batch : BatchMutation) : Unit =
        ++|(keyspace, batch, consistencyLevel)
        
    def ++|(keyspace : String, batch : BatchMutation, consistencyLevel : Int) :Unit =
        client.batch_insert(keyspace, batch, consistencyLevel)

    def --(keyspace : String, key : String, columnPathOrParent : ColumnPathOrParent, timestamp : Long) : Unit =
        --(keyspace,key,columnPathOrParent,timestamp,consistencyLevel)

    def --(keyspace : String, key : String, columnPathOrParent : ColumnPathOrParent, timestamp : Long, consistencyLevel : Int) : Unit =
        client.remove(keyspace, key, columnPathOrParent, timestamp, consistencyLevel)

    def /^(keyspace : String, key : String, columnFamily : String, start : Array[Byte], end : Array[Byte], ascending : Boolean, count : Int) : List[SuperColumn] =
        /^(keyspace,key,columnFamily,start,end,ascending,count,consistencyLevel)

    def /^(keyspace : String, key : String, columnFamily : String, start : Array[Byte], end : Array[Byte], isAscending : Boolean, count : Int, consistencyLevel : Int ) : List[SuperColumn] =
        client.get_slice_super(keyspace, key,columnFamily, start, end,isAscending,count,consistencyLevel).toList

    def /^(keyspace : String, key : String, columnFamily : String, superColNames : List[Array[Byte]]) : List[SuperColumn] =
        /^(keyspace,key,columnFamily,superColNames,consistencyLevel)

    def /^(keyspace : String, key : String, columnFamily : String, superColNames : List[Array[Byte]], consistencyLevel : Int) : List[SuperColumn] =
        client.get_slice_super_by_names(keyspace, key, columnFamily, superColNames.asJava,consistencyLevel).toList

    def |^(keyspace : String, key : String, superColumnPath : SuperColumnPath) : Option[SuperColumn] =
        |^(keyspace,key,superColumnPath,consistencyLevel)

    def |^(keyspace : String, key : String, superColumnPath : SuperColumnPath,consistencyLevel : Int) : Option[SuperColumn] =
        client.get_super_column(keyspace,key,superColumnPath,consistencyLevel)

    def ++|^ (keyspace : String, batch : BatchMutationSuper) : Unit =
        ++|^ (keyspace, batch,consistencyLevel)

    def ++|^ (keyspace : String, batch : BatchMutationSuper, consistencyLevel : Int) : Unit =
        client.batch_insert_super_column(keyspace, batch, consistencyLevel)

    def keys(keyspace : String, columnFamily : String, startsWith : String, stopsAt : String, maxResults : Option[Int]) : List[String] = {
        client.get_key_range(keyspace, columnFamily, startsWith, stopsAt, maxResults.getOrElse(-1)).toList
    }
    
    def property(name : String) : String = client.get_string_property(name)
    def properties(name : String) : List[String] = client.get_string_list_property(name).toList
    def describeTable(keyspace : String) = client.describe_keyspace(keyspace)

    def ?(query : String) = client.execute_query(query)
}

class Cassidy[T <: TTransport](transportPool : Pool[T], inputProtocol : Protocol, outputProtocol : Protocol, defConsistency : Int) extends Closeable
{
    def this(transportPool : Pool[T], ioProtocol : Protocol,consistencyLevel : Int) = this(transportPool,ioProtocol,ioProtocol,consistencyLevel)

    def newSession : Session = newSession(defConsistency)

    def newSession(consistency : Int) : Session = {
        val t = transportPool.borrowObject

        val c = new Cassandra.Client(inputProtocol(t),outputProtocol(t))

        new Session
        {
            val client = c
            val obtainedAt = System.currentTimeMillis
            val consistencyLevel = consistency //What's the sensible default?
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