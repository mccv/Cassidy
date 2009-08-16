package se.foldleft.cassidy

import org.apache.cassandra.service._

/**
 * Created by IntelliJ IDEA.
 * User: mmcbride
 * Date: Aug 16, 2009
 * Time: 12:55:45 PM
 * To change this template use File | Settings | File Templates.
 */

class CassidySuperColumnFamily(client : Cassandra.Client, keyspace : String, key : String, columnFamily : String, obtainedAt : Long, consistencyLevel : Int){

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  val columnParent = new ColumnParent(columnFamily,null)
  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None
  private implicit def colOrSuperListToSuperColList(l : List[ColumnOrSuperColumn]):List[SuperColumn] = l.map(_.super_column)
  private implicit def colOrSuperToSuperColOption(colOrSuper : ColumnOrSuperColumn):Option[SuperColumn] =
    if(colOrSuper != null) Some(colOrSuper.super_column) else None

  def /(columnName : Array[Byte]) = {
    new CassidySuperColumn(client,keyspace,key,columnFamily,columnName,obtainedAt,consistencyLevel)
  }
  def /(start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int) : List[SuperColumn] =
      /(start,end,ascending,count,consistencyLevel)

  def /(start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int, consistencyLevel : Int) : List[SuperColumn] = {
      val range = new SliceRange(start,end,ascending,count)
      /(new SlicePredicate(null,range), consistencyLevel)
  }

  def /(colNames : List[Array[Byte]]) : List[SuperColumn] =
      /(colNames,consistencyLevel)

  def /(colNames : List[Array[Byte]], consistencyLevel : Int) : List[SuperColumn] =
      /(new SlicePredicate(colNames.asJava,null),consistencyLevel)

  def /(predicate : SlicePredicate, consistencyLevel : Int) : List[SuperColumn] =
      client.get_slice(keyspace, key, columnParent, predicate, consistencyLevel).toList

  def |(columnName : Array[Byte]) : Option[SuperColumn] =
      |(columnName,consistencyLevel)

  def |(columnName : Array[Byte], consistencyLevel : Int) : Option[SuperColumn] =
      client.get(keyspace, key, new ColumnPath(columnFamily,null,columnName), consistencyLevel)

  def |#() : Int =
      |#(consistencyLevel)

  def |#(consistencyLevel : Int) : Int =
      client.get_count(keyspace, key, columnParent, consistencyLevel)

  def ++|(superColumn : Array[Byte], columnName : Array[Byte], value : Array[Byte]) : Unit =
      ++|(superColumn, columnName,value,obtainedAt,consistencyLevel)

  def ++|(superColumn : Array[Byte], columnName : Array[Byte], value : Array[Byte], timestamp : Long) : Unit =
      ++|(superColumn, columnName,value,timestamp,consistencyLevel)

  def ++|(superColumn : Array[Byte], columnName : Array[Byte], value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      client.insert(keyspace, key, new ColumnPath(columnFamily,superColumn,columnName), value,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long) : Unit =
      --(columnName,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long, consistencyLevel : Int) : Unit =
      client.remove(keyspace, key, new ColumnPath(columnFamily,null,columnName), timestamp, consistencyLevel)
}