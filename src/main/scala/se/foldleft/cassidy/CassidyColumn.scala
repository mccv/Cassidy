package se.foldleft.cassidy

import org.apache.cassandra.service._

class CassidyColumn(client : Cassandra.Client, keyspace : String, key : String, columnFamily : String, superColumnName : Option[Array[Byte]], columnName : Array[Byte], obtainedAt : Long, consistencyLevel : Int){

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  private implicit def colOrSuperToColOption(colOrSuper : ColumnOrSuperColumn):Option[Column] =
    if(colOrSuper != null) Some(colOrSuper.column) else None
  
  val columnParent = superColumnName match {
    case Some(name) => new ColumnParent(columnFamily,name)
    case None => new ColumnParent(columnFamily,null)
  }
  val columnPath = superColumnName match {
    case Some(name) => new ColumnPath(columnFamily,name,columnName)
    case None => new ColumnPath(columnFamily,null,columnName)
  }
  
  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None

  def |() : Option[Column] =
      |(consistencyLevel)

  def |(consistencyLevel : Int) : Option[Column] =
      client.get(keyspace, key, columnPath, consistencyLevel)

  def ++|(value : Array[Byte]) : Unit =
      ++|(value,obtainedAt,consistencyLevel)

  def ++|(value : Array[Byte], timestamp : Long) : Unit =
      ++|(value,timestamp,consistencyLevel)

  def ++|(value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      client.insert(keyspace, key, columnPath, value,timestamp,consistencyLevel)

  def --(timestamp : Long) : Unit =
      --(timestamp,consistencyLevel)

  def --(timestamp : Long, consistencyLevel : Int) : Unit =
      client.remove(keyspace, key, columnPath, timestamp, consistencyLevel)
}