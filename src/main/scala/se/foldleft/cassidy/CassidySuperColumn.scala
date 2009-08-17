package se.foldleft.cassidy

import org.apache.cassandra.service._

class CassidySuperColumn(superColumnFamily : CassidySuperColumnFamily, superColumnName : Array[Byte], obtainedAt : Long, consistencyLevel : Int)
 extends CassidyColumnParent[Column]{

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  val columnParent = new ColumnParent(superColumnFamily.columnFamily,superColumnName)
  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None
  private implicit def colOrSuperListToSuperColList(l : List[ColumnOrSuperColumn]):List[SuperColumn] = l.map(_.super_column)
  private implicit def colOrSuperToSuperColOption(colOrSuper : ColumnOrSuperColumn):Option[SuperColumn] =
    if(colOrSuper != null) Some(colOrSuper.super_column) else None

  def /(columnName : Array[Byte]) = {
    new CassidyColumn(this,columnName,obtainedAt,consistencyLevel)
  }
  def |() : Option[SuperColumn] =
      |(consistencyLevel)

  def |(consistencyLevel : Int) : Option[SuperColumn] =
      superColumnFamily|(superColumnName, consistencyLevel)

  def |(columnName : Array[Byte]) : Option[Column] =
      |(columnName,consistencyLevel)

  def |(columnName : Array[Byte], consistencyLevel : Int) : Option[Column] =
      superColumnFamily|(superColumnName,columnName, consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte]) : Unit =
      ++|(columnName,value,obtainedAt,consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long) : Unit =
      ++|(columnName,value,timestamp,consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      superColumnFamily++|(superColumnName, columnName, value,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long) : Unit =
      --(columnName,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long, consistencyLevel : Int) : Unit =
      superColumnFamily--(columnName, timestamp, consistencyLevel)
}