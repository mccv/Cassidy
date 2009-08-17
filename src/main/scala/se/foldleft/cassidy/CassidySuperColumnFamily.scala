package se.foldleft.cassidy

import org.apache.cassandra.service._

/**
 * Created by IntelliJ IDEA.
 * User: mmcbride
 * Date: Aug 16, 2009
 * Time: 12:55:45 PM
 * To change this template use File | Settings | File Templates.
 */

class CassidySuperColumnFamily(row : Row, val columnFamily : String, obtainedAt : Long, consistencyLevel : Int){

  import scala.collection.jcl.Conversions._
  import org.scala_tools.javautils.Imports._

  val columnParent = new ColumnParent(columnFamily,null)
  private implicit def null2Option[T](t : T) : Option[T] = if(t != null) Some(t) else None
  private implicit def colOrSuperListToSuperColList(l : List[ColumnOrSuperColumn]):List[SuperColumn] = l.map(_.super_column)
  private implicit def colOrSuperToSuperColOption(colOrSuper : ColumnOrSuperColumn):Option[SuperColumn] =
    if(colOrSuper != null) Some(colOrSuper.super_column) else None

  def /(columnName : Array[Byte]) = {
    new CassidySuperColumn(this,columnName,obtainedAt,consistencyLevel)
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
      row/(columnParent, predicate, consistencyLevel)

  def |(superColumnName : Array[Byte]) : Option[SuperColumn] =
      |(superColumnName,consistencyLevel)

  def |(superColumnName : Array[Byte], consistencyLevel : Int) : Option[SuperColumn] =
      row|(new ColumnPath(columnFamily,superColumnName,null), consistencyLevel) match {
        case Some(colOrSuperColumn) => {
          if(colOrSuperColumn.super_column != null) {
            Some(colOrSuperColumn.super_column)
          } else {
            throw new IllegalArgumentException("trying to treat " + columnFamily + " as a regular column family")
          }
        }
        case None => None
      }


  def |(superColumnName : Array[Byte], columnName : Array[Byte]) : Option[Column] =
      |(superColumnName,columnName,consistencyLevel)

  def |(superColumnName : Array[Byte], columnName : Array[Byte], consistencyLevel : Int) : Option[Column] =
      row|(new ColumnPath(columnFamily,superColumnName,columnName), consistencyLevel)  match {
        case Some(colOrSuperColumn) => {
          if(colOrSuperColumn.column != null) {
            Some(colOrSuperColumn.column)
          } else {
            throw new IllegalArgumentException("trying to treat " + columnFamily + ":" + superColumnName + " as a super column family")
          }
        }
        case None => None
      }

  def |#() : Int =
      |#(consistencyLevel)

  def |#(consistencyLevel : Int) : Int =
      row|#(columnParent, consistencyLevel)

  def ++|(superColumn : Array[Byte], columnName : Array[Byte], value : Array[Byte]) : Unit =
      ++|(superColumn, columnName,value,obtainedAt,consistencyLevel)

  def ++|(superColumn : Array[Byte], columnName : Array[Byte], value : Array[Byte], timestamp : Long) : Unit =
      ++|(superColumn, columnName,value,timestamp,consistencyLevel)

  def ++|(superColumn : Array[Byte], columnName : Array[Byte], value : Array[Byte], timestamp : Long, consistencyLevel : Int) =
      row++|(new ColumnPath(columnFamily,superColumn,columnName), value,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long) : Unit =
      --(columnName,timestamp,consistencyLevel)

  def --(columnName : Array[Byte], timestamp : Long, consistencyLevel : Int) : Unit =
      row--(new ColumnPath(columnFamily,null,columnName), timestamp, consistencyLevel)
}