package se.foldleft.cassidy

import org.apache.cassandra.service._

trait CassidyColumnParent[T]{
  def |(columnName : Array[Byte], consistencyLevel : Int) : Option[T]
  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long, consistencyLevel : Int) : Unit
  def --(columnName : Array[Byte], timestamp : Long, consistencyLevel : Int) : Unit
  val columnParent : ColumnParent  
}
class CassidyColumnFamily(row: Row, columnFamily : String, obtainedAt : Long, consistencyLevel : Int)
  extends CassidyColumnParent[Column]{

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  val columnParent = new ColumnParent(columnFamily,null)
  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None
  private implicit def colOrSuperListToColList(l : List[ColumnOrSuperColumn]):List[Column] = l.map(_.column)
  private implicit def colOrSuperToColOption(colOrSuper : ColumnOrSuperColumn):Option[Column] =
    if(colOrSuper != null) Some(colOrSuper.column) else None

  def /(columnName : Array[Byte]) = {
    new CassidyColumn(this,columnName,obtainedAt,consistencyLevel)
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
      row/(columnParent, predicate, consistencyLevel)

  def |(columnName : Array[Byte]) : Option[Column] =
      |(columnName,consistencyLevel)

  def |(columnName : Array[Byte], consistencyLevel : Int) : Option[Column] =
      row|(new ColumnPath(columnFamily,null,columnName), consistencyLevel) match {
        case Some(colOrSuperColumn) => {
          if(colOrSuperColumn.column != null) {
            Some(colOrSuperColumn.column)
          } else {
            throw new IllegalArgumentException("trying to treat " + columnFamily + " as a super column family")
          }
        }
        case None => None
      }

  def |#() : Int =
      |#(consistencyLevel)

  def |#(consistencyLevel : Int) : Int =
      row|#(columnParent, consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte]) : Unit =
      ++|(columnName,value,obtainedAt,consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long) : Unit =
      ++|(columnName,value,timestamp,consistencyLevel)

  def ++|(columnName : Array[Byte], value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      row++|(new ColumnPath(columnFamily,null,columnName), value,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long) : Unit =
      --(columnName,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long, consistencyLevel : Int) : Unit =
      row--(new ColumnPath(columnFamily,null,columnName), timestamp, consistencyLevel)
}