package indices

import org.scalatest._
import scala.util.Try

import scala.collection.mutable.{ Map => MMap }

class ExampleSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  class TestStore(map: MMap[String, Value]) extends KVStore with TableStore {
    def get(k: String): Option[Value] = map.get(k)
    def put(k: String, v: Value): Unit = map.update(k, v)
    def put(v: Seq[(String, Value)]) = v.foreach(x => put(x._1, x._2))
    def prefix(k: String) = map.filter(_._1.startsWith(k)).toList.iterator
    def closedb = ()
  }

  val backing = MMap[String, Value]()
  val store = new TestStore(backing)
  val tmp = java.io.File.createTempFile("dsfdsf", "dfsdf")
  println(tmp)
  tmp.delete
  val store2 = new LevelDBKVStore(tmp)
  test(store)
  test(store2)

  def indexChar(i: Int) = SimpleIndexer("last" + i, x => Try(x.document(i).toString))

  describe("stress") {
    it("x") {
      val table1 = Table(Keys.tables("abc"))
      def r = (0 until 30) map (i => scala.util.Random.nextPrintableChar) mkString
      val rs = 1 to 1000000 map (i => r)
      val t1 = System.nanoTime
      rs grouped (100) foreach { d =>
        store2.insert(d.map(d => Doc(d)), table1, List(indexChar(0), indexChar(1), indexChar(3)))
      }
      println((System.nanoTime - t1) / 1E9)
      println(store2.get(indexChar(0), "a", table1).size)
    }
  }
  def test(store: TableStore) = {

    describe("table" + store.hashCode) {

      def indexChars(is: List[Int]) = CompoundIndexer(is.map(indexChar))

      val table1 = Table(Keys.tables("abc"))
      val table2 = Table(Keys.tables("def"))
      it("table") {

        store.insert(Doc("a1"), table1, Nil)
        store.insert(Doc("a1"), table2, Nil)
        store.insert(Doc("a1"), table1, Nil)
        store.insert(Doc("a2"), table1, Nil)

        assert(backing("table_abc_pk_0") == Doc("a1"))
        assert(backing("table_abc_pk_2") == Doc("a1"))
        assert(backing("table_abc_pk_3") == Doc("a2"))
        // assert(backing("__lastid__") == RowId(3))
        assert(store.get(RowId(3L), table1) == Some(Doc("a2")))
        assert(store.get(RowId(2L), table2) == None)
        assert(store.get(RowId(1L), table2) == Some(Doc("a1")))
      }
      it("point index") {
        store.insert(Doc("a2") :: Doc("b2") :: Nil, table1, List(indexChar(1)))

        assert(store.get(indexChar(1), "2", table1) == List(Doc("a2"), Doc("b2")))

        store.insert(Doc("b23"), table1, List(indexChars(List(1, 2))))
        store.insert(Doc("a23"), table1, List(indexChars(List(1, 2))))
        store.insert(Doc("c24"), table1, List(indexChars(List(1, 2))))

        assert(store.get(indexChars(1 :: 2 :: Nil), List("2", "3"), table1) == List(Doc("b23"), Doc("a23")))

        assert(store.get(indexChar(0), "a", table1) == List())

        println(backing.toList.sortBy(_._1).mkString("\n"))

      }

    }

    // store.close
  }
}
