/*
 * Bridge.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package se.foldleft.pool

import org.apache.commons.pool._
import org.apache.commons.pool.impl._

trait PoolBridge[T,OP <: ObjectPool] extends Pool[T]
{
    val impl : OP
    override def borrowObject : T = impl.borrowObject.asInstanceOf[T]
    override def returnObject(t : T) = impl.returnObject(t)
    override def invalidateObject(t : T) = impl.invalidateObject(t)
    override def addObject = impl.addObject
    override def getNumIdle : Int = impl.getNumIdle
    override def getNumActive : Int = impl.getNumActive
    override def clear : Unit = impl.clear
    override def close : Unit = impl.close
    override def setFactory(factory : PoolItemFactory[T]) = impl.setFactory(toPoolableObjectFactory(factory))

    def toPoolableObjectFactory[T](pif : PoolItemFactory[T]) = new PoolableObjectFactory {
        def makeObject : Object = pif.makeObject.asInstanceOf[Object]
        def destroyObject(o : Object) : Unit = pif.destroyObject(o.asInstanceOf[T])
        def validateObject(o : Object) : Boolean = pif.validateObject(o.asInstanceOf[T])
        def activateObject(o : Object) : Unit = pif.activateObject(o.asInstanceOf[T])
        def passivateObject(o : Object) : Unit = pif.passivateObject(o.asInstanceOf[T])
    }

}

object StackPool
{
    def apply[T](factory : PoolItemFactory[T]) = new PoolBridge[T,StackObjectPool] {
        val impl = new StackObjectPool(toPoolableObjectFactory(factory))
    }

    def apply[T](factory : PoolItemFactory[T], maxIdle : Int) = new PoolBridge[T,StackObjectPool] {
        val impl = new StackObjectPool(toPoolableObjectFactory(factory),maxIdle)
    }

    def apply[T](factory : PoolItemFactory[T], maxIdle : Int, initIdleCapacity : Int) = new PoolBridge[T,StackObjectPool] {
        val impl = new StackObjectPool(toPoolableObjectFactory(factory),maxIdle,initIdleCapacity)
    }
}

object SoftRefPool
{
    def apply[T](factory : PoolItemFactory[T]) = new PoolBridge[T,SoftReferenceObjectPool] {
        val impl = new SoftReferenceObjectPool(toPoolableObjectFactory(factory))
    }

    def apply[T](factory : PoolItemFactory[T], initSize : Int) = new PoolBridge[T,SoftReferenceObjectPool] {
        val impl = new SoftReferenceObjectPool(toPoolableObjectFactory(factory),initSize)
    }

}
