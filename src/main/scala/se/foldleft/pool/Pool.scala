/**
 * se.foldleft.pool contain an abstract interface that mimics the API of org.apache.commons.pool
 * but adds typesafety
 */
package se.foldleft.pool

//ObjectPool
trait Pool[T] extends java.io.Closeable
{
    def borrowObject : T
    def returnObject(t : T) : Unit
    def invalidateObject(t : T) : Unit
    def addObject : Unit
    def getNumIdle : Int
    def getNumActive : Int
    def clear : Unit
    def setFactory(factory : PoolItemFactory[T]) : Unit
}

//ObjectPoolFactory
trait PoolFactory[T]
{
    def createPool : Pool[T]
}

//PoolableObjectFactory
trait PoolItemFactory[T]
{
    def makeObject : T
    def destroyObject(t : T) : Unit
    def validateObject(t : T) : Boolean
    def activateObject(t : T) : Unit
    def passivateObject(t : T) : Unit
}