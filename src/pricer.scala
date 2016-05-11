/**
  * Pricer, reads a market data log on standard input. As the book is modified, Pricer prints (on standard output).
  * the total expense you would incur if you bought target-size shares (by taking as many asks as necessary, lowest first),
  * and the total income you would receive if you sold target-size shares (by hitting as many bids as necessary, highest first).
  * Each time the income or expense changes, it prints the changed value.
  *
  * Pricer takes one command-line argument: target-size.
  *
  * @author Bradley Harrington (bharring@gmail.com), Feb. 2016
  */

import scala.io.Source._
import scala.collection.mutable
import scala.collection.mutable.SortedSet
import scala.annotation.tailrec

/**
  * The fundamental order entity used to track pricing changes
  *
  * @param orderId A unique string that subsequent "Reduce Order" messages will use to modify this order.
  * @param price The limit price of this order.
  * @param size The size in shares of this order, when it was initially sent to the market by some stock trader.
  *
  */
case class Order(orderId: String, price: Double, size: Int) extends Ordered[Order] {
  // Required method for implementing Ordering. Orders must take both price and orderId into consideration when sorting,
  // otherwise adds will have unintended side-effects such as overwriting orders with the same price.
  def compare(that: Order): Int = (this.price compare that.price) compare (this.orderId compare that.orderId)
}

/**
  * An extension of the mutable.SortedSet collection, object containing static methods.
  */
object SortedOrderBook {
  // Collection constructor, with explicitly defined custom ordering
  def apply[A <: Order]()(implicit ord: scala.Ordering[A]) = new SortedOrderBook[A](new SortedOrderBook(SortedSet[A]()(ord)))
}

/**
  * An extension of the mutable.SortedSet collection, class containing non-static methods.
  */
class SortedOrderBook[A <: Order](self: mutable.SortedSet[A]) extends mutable.SortedSet[A] {
  // Methods required to be explicitly implemented when extending mutable.SortedSet. No changes to default behaviors were made.
  def contains(elem: A): Boolean = self.contains(elem)
  def +=(elem: A): this.type = { self.add(elem); this }
  def -=(elem: A): this.type = { self.remove(elem); this }
  override def iterator: Iterator[A] = self.iterator
  implicit val ordering: Ordering[A] = self.ordering
  override def rangeImpl(from: Option[A], until: Option[A]): mutable.SortedSet[A] = this.rangeImpl(from, until)
  override def keysIteratorFrom(start: A): Iterator[A] = this.keysIteratorFrom(start)

  /**
    * Searches the current market data log for the optimum price, using tail recursion to maintain a small memory footprint
    *
    * @param targetSize Number of shares to buy or sell, to constrain availability of shares at a given price
    * @return The total expense you would incur if you bought targetSize shares (assuming the implicit ordering for bids or asks)
    *         or zero (0) if the transaction is not possible
    */
  def getBestPrice(targetSize: Int): Double = {
    @tailrec
    def loop(it: Iterator[A], price: Double, remaining: Int): Double = {
      if (!it.hasNext && remaining > 0) 0 // The iterator is exhausted and the order cannot be fulfilled
      else if (remaining <= 0) BigDecimal(price).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble // The order can be executed!
      else {
        val order = it.next()
        val shares = math.min(remaining, order.size)
        loop(it, price + shares * order.price, remaining - shares)
      }
    }
    loop(this.iterator, 0, targetSize)
  }
}

/**
  * Order Book class, extending SortedOrderBook, for use with each order type (Ask & Bid)
  *
  * @param book pre-instantiated SortedSet
  */
class Book(book: mutable.SortedSet[Order]) extends SortedOrderBook[Order](book: mutable.SortedSet[Order]) {
  var prevPrice: Double = 0 // previously identified best price for this book
  var targetSize: Int = 0 // target-size from command line
  var transType: String = "" // transaction type string for streaming to the output log

  /**
    * Reduces order size by amount indicated or removes from book if amount would be zero
    *
    * @param orderId Unique order identifier within the order book
    * @param size The number of orders to remove from the order book
    * @return true if orderId was found, otherwise false
    */
  def reduceOrder(orderId: String, size: Int): Boolean = {
    find { o => o.orderId == orderId } match {
      case Some(order: Order) => {
        this -= order // remove old order
        // re-add new order with new size, because Order is immutable
        if (order.size > size) this += Order(order.orderId, order.price, order.size - size)
        true // tell caller order was found
      }
      case None => false // tell caller order was not found
    }
  }

  /**
    * Prints an output log message if the current best price is different from the previously printed log output message
    *
    * @param timestamp Timestamp of the current order being added or reduces. For use in streaming to the output log
    */
  def maybePrintNewPrice(timestamp: String) = {
    val price = getBestPrice(targetSize)
    if (prevPrice != price) {
      if (price > 0) System.out.println(timestamp + transType + f"$price%.2f")
      else System.out.println(timestamp + transType + "NA")
      prevPrice = price
    }
  }
}

/**
  * Encapsulates each book type (Ask & Bid) and evaluates each incoming log message
  *
  * @param _targetSize target-size from command line
  */
class OrderBook(_targetSize: Int) {
  // Separate book for tracking ask orders. Dividing the order book into two parts
  // greatly improves performance while adding a modest amount of complexity
  var askBook = new Book(SortedSet[Order]()(Ordering.by(order => (order.price, order.orderId)))) {
    transType = " B "
    targetSize = _targetSize
  }
  // Separate book for tracking bid orders.
  var bidBook = new Book(SortedSet[Order]()(Ordering.by(order => (-order.price, order.orderId)))) {
    transType = " S "
    targetSize = _targetSize
  }
  // combined books
  val orderBook = List[Book](askBook, bidBook)

  /**
    * Identifies which book contains the order to be reduced,
    * and then maybe prints the new price only from that book
    *
    * @param timestamp timestamp of current incoming log message
    * @param orderId unique order identifier, from current incoming log message
    * @param size number of shares to add or reduce, from current incoming log message
    */
  def reduceOrder(timestamp: String, orderId: String, size: Int) = {
    orderBook.find(b => b.reduceOrder(orderId, size)) match {
      case Some(book: Book) => book.maybePrintNewPrice(timestamp)
      case None =>
    }
  }

  /**
    * Accepts market log messages from standard input. For add messages, routes to the appropriate book and maybe prints
    * the best price from that book. For reduce messages, routes to the reduceOrder() method. For any other type of input
    * displays a warning message to standard error, and moves on to the next message.
    */
  def parseMessages() {
    System.setIn(new java.io.FileInputStream("pricer.in"))
    stdin.getLines.toList.foreach(order => {
      order.split(" ").toList match {
        case timestamp :: "A" :: orderId :: "B" :: price :: size :: Nil => {
          bidBook += Order(orderId, price.toDouble, size.toInt)
          bidBook.maybePrintNewPrice(timestamp)
        }
        case timestamp :: "A" :: orderId :: "S" :: price :: size :: Nil => {
          askBook += Order(orderId, price.toDouble, size.toInt)
          askBook.maybePrintNewPrice(timestamp)
        }
        case timestamp :: "R" :: orderId :: size :: Nil => reduceOrder(timestamp, orderId, size.toInt)
        case _ => System.err.println("Warning: invalid message: " + order)
      }
    })
  }
}

object pricer {
  // message to be displayed if passed incorrect command arguments
  val usage =
    """
      Usage: pricer TARGET_SIZE
      Where TARGET_SIZE is the target number of shares to trade.
      Expects market data log from standard input, and sends results to standard output.
    """

  /**
    * pricer entry point.
    *
    * @param args the command line arguments
    */
  def main(args: Array[String]) = {
    /**
      * Safely parse string to Int
      *
      * @param in string to parse
      * @return corresponding integer value, or zero if parse fails
      */
    def toInt(in: String): Int = {
      try {
        Integer.parseInt(in.trim)
      } catch {
        case e: NumberFormatException => 0
      }
    }
    /**
      * Returns integer value of first command line argument, if any.
      *
      * @param args command line arguments
      * @return Integer value of first command line argument, or zero
      */
    def processArgs(args: Array[String]) = args match {
      case Array(i, _*) => toInt(i)
      case _ => 0
    }
    val targetSize = processArgs(args)
    if (targetSize > 0) new OrderBook(targetSize).parseMessages()
    else System.err.println(usage)
  }
}