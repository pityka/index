package indices

import org.scalatest._
import scala.util.Try

import scala.collection.mutable.{ Map => MMap }

class ExampleSpec extends FunSpec with Matchers {

  class TestStore(map: MMap[String, Value]) extends KVStore with TableStore {
    def get(k: String): Option[Value] = map.get(k)
    def put(k: String, v: Value): Unit = map.update(k, v)
    def append(k: String, v: RowId): Unit = map.get(k) match {
      case None => map.update(k, ListOfRowIds(List(v)))
      case Some(ListOfRowIds(x)) => map.update(k, ListOfRowIds(v :: x))
    }
    def listPrefix(k: String) = map.filter(_._1.startsWith(k)).toList.iterator
  }

  describe("table") {
    def indexChar(i: Int) = SimpleIndexer("last" + i, x => Try(x.document(i).toString))

    def indexChars(is: List[Int]) = CompoundIndexer(is.map(indexChar))
    val backing = MMap[String, Value]()
    val store = new TestStore(backing)
    val table1 = Table(Keys.tables("abc"))
    val table2 = Table(Keys.tables("def"))
    it("table") {

      store.insert(Doc("a1"), table1, Nil)
      store.insert(Doc("a1"), table2, Nil)
      store.insert(Doc("a1"), table1, Nil)
      store.insert(Doc("a2"), table1, Nil)

      assert(backing("table_abc_pk_0") == Doc("a1"))
      assert(backing("table_abc_pk_1") == Doc("a1"))
      assert(backing("table_abc_pk_2") == Doc("a2"))
      assert(backing("table_abc__lastid__") == RowId(2))
      assert(store.get(RowId(2L), table1) == Some(Doc("a2")))
      assert(store.get(RowId(2L), table2) == None)
      assert(store.get(RowId(0L), table2) == Some(Doc("a1")))
    }
    it("point index") {
      store.insert(Doc("a2"), table1, List(indexChar(1)))
      store.insert(Doc("b2"), table1, List(indexChar(1)))
      assert(store.get(indexChar(1), "2", table1) == List(Doc("b2"), Doc("a2")))

      store.insert(Doc("b23"), table1, List(indexChars(List(1, 2))))
      store.insert(Doc("a23"), table1, List(indexChars(List(1, 2))))
      store.insert(Doc("c24"), table1, List(indexChars(List(1, 2))))

      assert(store.get(indexChars(1 :: 2 :: Nil), List("2", "3"), table1) == List(Doc("a23"), Doc("b23")))

      store.indexAll(table1, indexChar(0) :: Nil)
      assert(store.get(indexChar(0), "a", table1) == List(Doc("a2"), Doc("a23"), Doc("a2"), Doc("a1"), Doc("a1")))

      println(backing.toList.sortBy(_._1).mkString("\n"))

    }

  }
}
