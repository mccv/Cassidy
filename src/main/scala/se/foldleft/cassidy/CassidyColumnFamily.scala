package se.foldleft.cassidy

import org.apache.cassandra.service._

/**
 * Created by IntelliJ IDEA.
 * User: mmcbride
 * Date: Aug 16, 2009
 * Time: 12:55:45 PM
 * To change this template use File | Settings | File Templates.
 */

class CassidyColumnFamily(client : Cassandra.Client, keyspace : String, key : String, columnFamily : String, obtainedAt : Long, consistencyLevel : Int){

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  val columnParent = new ColumnParent(columnFamily,null)
  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None
  private implicit def colOrSuperListToColList(l : List[ColumnOrSuperColumn]):List[Column] = l.map(_.column)
  private implicit def colOrSuperToColOption(colOrSuper : ColumnOrSuperColumn):Option[Column] =
    if(colOrSuper != null) Some(colOrSuper.column) else None

  def /(columnName : Array[Byte]) = {
    new CassidyColumn(client,keyspace,key,columnFamily,None,columnName,obtainedAt,consistencyLevel)
  }
  def /(start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int) : List[Column] =
      /(start,end,ascending,count,consistencyLevel)

  def /(start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int, consistencyLevel : Int) : List[Column] = {
      val range = new SliceRange(start,end,ascending,count)
      /(new SlicePredicate(null,range), consistencyLevel)
  }

  def /(colNames : List[Array[Byte]]) : List[Column] =
      /(colNames,consistencyLevel)

  def /(colNames : List[Array[Byte]], consistencyLevel : Int) : List[Column] =
      /(new SlicePredicate(colNames.asJava,null),consistencyLevel)

  def /(predicate : SlicePredicate, consistencyLevel : Int) : List[Column] =
      client.get_slice(keyspace, key, columnParent, predicate, consistencyLevel).toList

  def |(columnName : Array[Byte]) : Option[Column] =
      |(columnName,consistencyLevel)

  def |(columnName : Array[Byte], consistencyLevel : Int) : Option[Column] =
      client.get(keyspace, key, new ColumnPath(columnFamily,null,columnName), consistencyLevel)

  def |#() : Int =
      |#(consistencyLevel)

  def |#(consistencyLevel : Int) : Int =
      client.get_count(keyspace, key, columnParent, consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte]) : Unit =
      ++|(columnName,value,obtainedAt,consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long) : Unit =
      ++|(columnName,value,timestamp,consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      client.insert(keyspace, key, new ColumnPath(columnFamily,null,columnName), value,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long) : Unit =
      --(columnName,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long, consistencyLevel : Int) : Unit =
      client.remove(keyspace, key, new ColumnPath(columnFamily,null,columnName), timestamp, consistencyLevel)
}