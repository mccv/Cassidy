package se.foldleft.cassidy

import org.apache.cassandra.service._

class CassidySuperColumn(client : Cassandra.Client, keyspace : String, key : String, columnFamily : String, superColumnName : Array[Byte], obtainedAt : Long, consistencyLevel : Int){

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  val columnParent = new ColumnParent(columnFamily,superColumnName)
  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None
  private implicit def colOrSuperListToSuperColList(l : List[ColumnOrSuperColumn]):List[SuperColumn] = l.map(_.super_column)
  private implicit def colOrSuperToSuperColOption(colOrSuper : ColumnOrSuperColumn):Option[SuperColumn] =
    if(colOrSuper != null) Some(colOrSuper.super_column) else None

  def /(columnName : Array[Byte]) = {
    new CassidyColumn(client,keyspace,key,columnFamily,Some(columnName),columnName,obtainedAt,consistencyLevel)
  }
  def |(columnName : Array[Byte]) : Option[SuperColumn] =
      |(columnName,consistencyLevel)

  def |(columnName : Array[Byte], consistencyLevel : Int) : Option[SuperColumn] =
      client.get(keyspace, key, new ColumnPath(columnFamily,superColumnName,columnName), consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte]) : Unit =
      ++|(columnName,value,obtainedAt,consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long) : Unit =
      ++|(columnName,value,timestamp,consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      client.insert(keyspace, key, new ColumnPath(columnFamily,superColumnName,columnName), value,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long) : Unit =
      --(columnName,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long, consistencyLevel : Int) : Unit =
      client.remove(keyspace, key, new ColumnPath(columnFamily,superColumnName,columnName), timestamp, consistencyLevel)
}