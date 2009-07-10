/**
 * TransportPool enables pooling of Thrift TTransports
 */

package se.foldleft.cassidy

import se.foldleft.pool._
import org.apache.thrift.transport._


trait TransportFactory[T <: TTransport] extends PoolItemFactory[T]
{
    def createTransport : T

    def makeObject : T = createTransport
    def destroyObject(transport : T) : Unit = transport.close
    def validateObject(transport : T) = transport.isOpen
    def activateObject(transport : T) : Unit = if( !transport.isOpen ) transport.open else ()
    def passivateObject(transport : T) : Unit = transport.flush
}

case class SocketProvider(val host : String,val port : Int) extends TransportFactory[TSocket]
{
    def createTransport = new TSocket(host,port)
}