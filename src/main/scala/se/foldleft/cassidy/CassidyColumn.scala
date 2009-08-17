package se.foldleft.cassidy

import org.apache.cassandra.service._

class CassidyColumn(columnFamily : CassidyColumnParent[Column], columnName : Array[Byte], obtainedAt : Long, consistencyLevel : Int){

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  private implicit def colOrSuperToColOption(colOrSuper : ColumnOrSuperColumn):Option[Column] =
    if(colOrSuper != null) Some(colOrSuper.column) else None
  
  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None

  def |() : Option[Column] =
      |(consistencyLevel)

  def |(consistencyLevel : Int) : Option[Column] =
      columnFamily|(columnName, consistencyLevel)

  def ++|(value : Array[Byte]) : Unit =
      ++|(value,obtainedAt,consistencyLevel)

  def ++|(value : Array[Byte], timestamp : Long) : Unit =
      ++|(value,timestamp,consistencyLevel)

  def ++|(value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      columnFamily++|(columnName, value,timestamp,consistencyLevel)

  def --(timestamp : Long) : Unit =
      --(timestamp,consistencyLevel)

  def --(timestamp : Long, consistencyLevel : Int) : Unit =
      columnFamily--(columnName, timestamp, consistencyLevel)
}