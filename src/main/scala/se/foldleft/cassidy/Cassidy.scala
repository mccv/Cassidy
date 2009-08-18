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
import scala.collection.mutable.{Map,HashMap}

trait Session extends Closeable with Flushable
{
    import scala.collection.jcl.Conversions._
    import org.scala_tools.javautils.Imports._

    private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None

    protected val client : Cassandra.Client

    val obtainedAt : Long
    val consistencyLevel : Int

    def /(keyspace : String) = new KeySpace(this,keyspace,obtainedAt,consistencyLevel)

    def /(keyspace : String, key : String, columnParent : ColumnParent, start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int) : List[ColumnOrSuperColumn] =
        /(keyspace,key,columnParent,start,end,ascending,count,consistencyLevel)

    def /(keyspace : String, key : String, columnParent : ColumnParent, start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int, consistencyLevel : Int) : List[ColumnOrSuperColumn] = {
        val range = new SliceRange(start,end,ascending,count)
        /(keyspace, key, columnParent, new SlicePredicate(null,range), consistencyLevel)
    }

    def /(keyspace : String, key : String, columnParent : ColumnParent, colNames : List[Array[Byte]]) : List[ColumnOrSuperColumn] =
        /(keyspace,key,columnParent,colNames,consistencyLevel)

    def /(keyspace : String, key : String, columnParent : ColumnParent, colNames : List[Array[Byte]], consistencyLevel : Int) : List[ColumnOrSuperColumn] =
        /(keyspace,key,columnParent,new SlicePredicate(colNames.asJava,null),consistencyLevel)

    def /(keyspace : String, key : String, columnParent : ColumnParent, predicate : SlicePredicate, consistencyLevel : Int) : List[ColumnOrSuperColumn] =
        client.get_slice(keyspace, key, columnParent, predicate, consistencyLevel).toList

    def |(keyspace : String, key : String, colPath : ColumnPath) : Option[ColumnOrSuperColumn] =
        |(keyspace,key,colPath,consistencyLevel)

    def |(keyspace : String, key : String, colPath : ColumnPath, consistencyLevel : Int) : Option[ColumnOrSuperColumn] =
        client.get(keyspace, key, colPath, consistencyLevel)

    def |#(keyspace : String, key : String, columnParent : ColumnParent) : Int =
        |#(keyspace,key,columnParent,consistencyLevel)

    def |#(keyspace : String, key : String, columnParent : ColumnParent, consistencyLevel : Int) : Int =
        client.get_count(keyspace, key, columnParent, consistencyLevel)

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

    def ++|^(keyspace : String, batch : BatchMutationSuper) : Unit =
        ++|^(keyspace, batch, consistencyLevel)

    def ++|^(keyspace : String, batch : BatchMutationSuper, consistencyLevel : Int) :Unit =
        client.batch_insert_super_column(keyspace, batch, consistencyLevel)

    def --(keyspace : String, key : String, columnPath : ColumnPath, timestamp : Long) : Unit =
        --(keyspace,key,columnPath,timestamp,consistencyLevel)

    def --(keyspace : String, key : String, columnPath : ColumnPath, timestamp : Long, consistencyLevel : Int) : Unit =
        client.remove(keyspace, key, columnPath, timestamp, consistencyLevel)

    def keys(keyspace : String, columnFamily : String, startsWith : String, stopsAt : String, maxResults : Option[Int]) : List[String] = {
        client.get_key_range(keyspace, columnFamily, startsWith, stopsAt, maxResults.getOrElse(-1)).toList
    }
    
    def property(name : String) : String = client.get_string_property(name)
    def properties(name : String) : List[String] = client.get_string_list_property(name).toList
    def describeTable(keyspace : String) = client.describe_keyspace(keyspace)

}

/**
 * BatchSession coalesces inserts into a set of BatchMutation and/or BatchMutationSuper objects.
 * When flush() is called these batch operations are applied.
 */
trait BatchSession extends Session {
  // keep track of the various batch operations we'll eventually commit
  val batchMap = HashMap[String,Map[String,BatchMutation]]()
  val superBatchMap = HashMap[String,Map[String,BatchMutationSuper]]()

  import scala.collection.jcl.Conversions._
  
  override def ++|(keyspace : String, key : String, columnPath : ColumnPath, value : Array[Byte], timestamp : Long, consistencyLevel : Int) = {
    if(columnPath != null && columnPath.column != null && columnPath.column_family != null){
      // normal and super columns get handled differently
      if(columnPath.super_column == null){
        insertNormal(keyspace,key,columnPath,value,timestamp)
      } else {
        insertSuper(keyspace,key,columnPath,value,timestamp)
      }
    }else{
      throw new IllegalArgumentException("incomplete column path " + columnPath)
    }
  }

  def insertNormal(keyspace: String, id: String, columnPath : ColumnPath, value : Array[Byte], timestamp : Long) = {
    // add an entry to batchMap... this feels overly convoluted
    val keyspaceMap = batchMap.getOrElseUpdate(keyspace,HashMap[String,BatchMutation]())
    val batch = keyspaceMap.getOrElseUpdate(id,new BatchMutation())

    // make sure we get a good, initialized BatchMutation out of our map
    batch.key = id
    if(batch.cfmap == null){
      batch.cfmap = new java.util.HashMap[java.lang.String,java.util.List[Column]]()
    }
    if(batch.cfmap.get(columnPath.column_family) == null){
      batch.cfmap.put(columnPath.column_family, new java.util.ArrayList[Column]())
    }

    // now add our new column to the list
    val colTList = batch.cfmap.get(columnPath.column_family)
    val colT = new Column()
    colT.name = columnPath.column
    colT.timestamp = timestamp
    colT.value = value
    colTList.add(colT)
  }


  def insertSuper(keyspace: String, id: String, columnPath : ColumnPath, value: Array[Byte], timestamp: Long) = {
    // if insertNormal feels overly convoluted, this one reeks of overconvolution
    val keyspaceMap = superBatchMap.getOrElseUpdate(keyspace,Map[String,BatchMutationSuper]())
    val batch = keyspaceMap.getOrElseUpdate(id,new BatchMutationSuper())

    // make sure we get a good, initilazed BatchMutationSuper out of our map
    batch.key = id
    if(batch.cfmap == null){
      batch.cfmap = new java.util.HashMap[java.lang.String,java.util.List[SuperColumn]]()
    }
    if(batch.cfmap.get(columnPath.column_family) == null){
      batch.cfmap.put(columnPath.column_family, new java.util.ArrayList[SuperColumn]())
    }

    // now make sure we get a good SuperColumn out of our BatchMutationSuper
    val superColTList = batch.cfmap.get(columnPath.column_family)
    val superColT = superColTList.find(_.name == columnPath.super_column) match {
      case Some(superCol) => superCol
      case None => {
        val superCol = new SuperColumn()
        superCol.name = columnPath.super_column
        superCol.columns = new java.util.ArrayList[Column]()
        superColTList.add(superCol)
        superCol
      }
    }

    // and finally, add our new column
    val colT = new Column
    colT.name = columnPath.column
    colT.timestamp = timestamp
    colT.value = value
    superColT.columns.add(colT)
  }

  /**
   * applies all batch operations in batchMap and superBatchMap, then clears out
   * those maps
   */
  def flush():Unit = {
    batchMap.foreach((keyspaceEntry:Tuple2[String,Map[String,BatchMutation]]) => {
      keyspaceEntry._2.foreach((entry:Tuple2[String,BatchMutation]) => {
        /*println("batch mutation for keyspace " + keyspaceEntry._1)
        println("batch mutation key " + entry._2.key)
        entry._2.cfmap.foreach((cfmap) => {
          cfmap._2.foreach((col) => {
            println("\tcol name = " + col.name)
            println("\tcol value = " + new String(col.value))
            println("\tcol ts = " + col.timestamp)
          })
        })*/
        ++|(keyspaceEntry._1,entry._2,consistencyLevel)
      })
    })
    batchMap.clear()
    superBatchMap.foreach((keyspaceEntry:Tuple2[String,Map[String,BatchMutationSuper]]) => {
      keyspaceEntry._2.foreach((entry:Tuple2[String,BatchMutationSuper]) => {
        ++|^(keyspaceEntry._1,entry._2,consistencyLevel)
      })
    })
    superBatchMap.clear()
  }



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

    def newBatchSession : Session = newBatchSession(defConsistency)
  
    def newBatchSession(consistency : Int) : Session = {
        val t = transportPool.borrowObject

        val c = new Cassandra.Client(inputProtocol(t),outputProtocol(t))

        new BatchSession
        {
            val client = c
            val obtainedAt = System.currentTimeMillis
            val consistencyLevel = consistency //What's the sensible default?
            override def flush = {super.flush(); t.flush()}
            def close = transportPool.returnObject(t)
        }
    }
    def doWork[R](work : (Session) => R):R = {
        val s = newSession
        doWork(s,work)
    }

    def doBatchWork[R](work : (Session) => R):R = {
      val s = newBatchSession
      doWork(s,work)
    }

    protected def doWork[R](s : Session, work : (Session) => R):R = {
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