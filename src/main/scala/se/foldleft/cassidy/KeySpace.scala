package se.foldleft.cassidy

import org.apache.cassandra.service._

class KeySpace(session : Session, keyspace : String, obtainedAt : Long, consistencyLevel : Int){

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None

  def /(key : String) = new Row(this,key,obtainedAt,consistencyLevel)
  def /(key : String, columnParent : ColumnParent, start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int) : List[ColumnOrSuperColumn] =
      /(key,columnParent,start,end,ascending,count,consistencyLevel)

  def /(key : String, columnParent : ColumnParent, start : Array[Byte],end : Array[Byte], ascending : Boolean, count : Int, consistencyLevel : Int) : List[ColumnOrSuperColumn] = {
      val range = new SliceRange(start,end,ascending,count)
      /(key, columnParent, new SlicePredicate(null,range), consistencyLevel)
  }

  def /(key : String, columnParent : ColumnParent, colNames : List[Array[Byte]]) : List[ColumnOrSuperColumn] =
      /(key,columnParent,colNames,consistencyLevel)

  def /(key : String, columnParent : ColumnParent, colNames : List[Array[Byte]], consistencyLevel : Int) : List[ColumnOrSuperColumn] =
      /(key,columnParent,new SlicePredicate(colNames.asJava,null),consistencyLevel)

  def /(key : String, columnParent : ColumnParent, predicate : SlicePredicate, consistencyLevel : Int) : List[ColumnOrSuperColumn] =
      session/(keyspace, key, columnParent, predicate, consistencyLevel)

  def |(key : String, colPath : ColumnPath) : Option[ColumnOrSuperColumn] =
      |(key,colPath,consistencyLevel)

  def |(key : String, colPath : ColumnPath, consistencyLevel : Int) : Option[ColumnOrSuperColumn] =
      session|(keyspace, key, colPath, consistencyLevel)

  def |#(key : String, columnParent : ColumnParent) : Int =
      |#(key,columnParent,consistencyLevel)

  def |#(key : String, columnParent : ColumnParent, consistencyLevel : Int) : Int =
      session|#(keyspace, key, columnParent, consistencyLevel)

  def ++|(key : String, columnPath : ColumnPath, value : Array[Byte]) : Unit =
      ++|(key,columnPath,value,obtainedAt,consistencyLevel)

  def ++|(key : String, columnPath : ColumnPath, value : Array[Byte], timestamp : Long) : Unit =
      ++|(key,columnPath,value,timestamp,consistencyLevel)

  def ++|(key : String, columnPath : ColumnPath, value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      session++|(keyspace, key, columnPath, value,timestamp,consistencyLevel)

  def ++|(batch : BatchMutation) : Unit =
      ++|(batch, consistencyLevel)

  def ++|(batch : BatchMutation, consistencyLevel : Int) :Unit =
      session++|(keyspace, batch, consistencyLevel)

  def ++|^(batch : BatchMutationSuper) : Unit =
      ++|^(batch, consistencyLevel)

  def ++|^(batch : BatchMutationSuper, consistencyLevel : Int) :Unit =
      session++|^(keyspace, batch, consistencyLevel)

  def --(key : String, columnPath : ColumnPath, timestamp : Long) : Unit =
      --(key,columnPath,timestamp,consistencyLevel)

  def --(key : String, columnPath : ColumnPath, timestamp : Long, consistencyLevel : Int) : Unit =
      session--(keyspace, key, columnPath, timestamp, consistencyLevel)

  def keys(columnFamily : String, startsWith : String, stopsAt : String, maxResults : Option[Int]) : List[String] = {
      session.keys(keyspace, columnFamily, startsWith, stopsAt, maxResults)
  }

  def describeTable() = session.describeTable(keyspace)

}