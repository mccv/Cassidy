package se.foldleft.cassidy

import org.apache.cassandra.service._

class Row(client : Cassandra.Client, keyspace : String, key : String, obtainedAt : Long, consistencyLevel : Int){

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None

  def /(columnFamily : String) = {
    new CassidyColumnFamily(client,keyspace,key,columnFamily,obtainedAt,consistencyLevel)
  }

  def *(columnFamily : String) = {
    new CassidySuperColumnFamily(client,keyspace,key,columnFamily,obtainedAt,consistencyLevel)
  }
  def /(columnParent : ColumnParent, start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int) : List[ColumnOrSuperColumn] =
      /(columnParent,start,end,ascending,count,consistencyLevel)

  def /(columnParent : ColumnParent, start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int, consistencyLevel : Int) : List[ColumnOrSuperColumn] = {
      val range = new SliceRange(start,end,ascending,count)
      /(columnParent, new SlicePredicate(null,range), consistencyLevel)
  }

  def /(columnParent : ColumnParent, colNames : List[Array[Byte]]) : List[ColumnOrSuperColumn] =
      /(columnParent,colNames,consistencyLevel)

  def /(columnParent : ColumnParent, colNames : List[Array[Byte]], consistencyLevel : Int) : List[ColumnOrSuperColumn] =
      /(columnParent,new SlicePredicate(colNames.asJava,null),consistencyLevel)

  def /(columnParent : ColumnParent, predicate : SlicePredicate, consistencyLevel : Int) : List[ColumnOrSuperColumn] =
      client.get_slice(keyspace, key, columnParent, predicate, consistencyLevel).toList

  def |(colPath : ColumnPath) : Option[ColumnOrSuperColumn] =
      |(colPath,consistencyLevel)

  def |(colPath : ColumnPath, consistencyLevel : Int) : Option[ColumnOrSuperColumn] =
      client.get(keyspace, key, colPath, consistencyLevel)

  def |#(columnParent : ColumnParent) : Int =
      |#(columnParent,consistencyLevel)

  def |#(columnParent : ColumnParent, consistencyLevel : Int) : Int =
      client.get_count(keyspace, key, columnParent, consistencyLevel)

  def ++|(columnPath : ColumnPath, value : Array[Byte]) : Unit =
      ++|(columnPath,value,obtainedAt,consistencyLevel)

  def ++|(columnPath : ColumnPath, value : Array[Byte], timestamp : Long) : Unit =
      ++|(columnPath,value,timestamp,consistencyLevel)

  def ++|(columnPath : ColumnPath, value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      client.insert(keyspace, key, columnPath, value,timestamp,consistencyLevel)

  def --(columnPath : ColumnPath, timestamp : Long) : Unit =
      --(columnPath,timestamp,consistencyLevel)

  def --(columnPath : ColumnPath, timestamp : Long, consistencyLevel : Int) : Unit =
      client.remove(keyspace, key, columnPath, timestamp, consistencyLevel)
}