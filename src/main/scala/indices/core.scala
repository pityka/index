package indices

import scala.util.Try

sealed trait Value

case class RowId(id: Long) extends Value { override def toString = id.toString }
case class Doc(document: String) extends Value
case class ListOfRowIds(values: List[RowId]) extends Value

trait KVStore {
  def get(k: String): Option[Value]
  def put(k: String, v: Value): Unit
  def append(k: String, v: RowId): Unit
  def listPrefix(k: String): Iterator[(String, Value)]
}

trait ValueKeyFactory {
  def value(value: String): String
}

trait KeyFactory {
  def suffix(value: String): KeyFactory
  def value: ValueKeyFactory
}

case class Composite(segments: Vector[String]) extends KeyFactory {
  def suffix(value: String) = Composite(segments :+ value)
  def value = CompositeEnd(segments)
}
object Keys {
  val root = Composite(Vector())
  def tables(s: String) = root.suffix("table").suffix(s)
}
case class CompositeEnd(segments: Vector[String]) extends ValueKeyFactory {
  def value(value: String) = (segments :+ value).mkString("_")
}

case class Table(name: KeyFactory) {
  val pk: ValueKeyFactory = name.suffix("pk").value
  val lastId: String = name.suffix("_lastid_").value.value("")
}

case class IndexKey(name: KeyFactory) {
  def compound(n: String) = IndexKey(name.suffix("index").suffix(n))
}

trait Indexer[-Query] {
  def insert(store: KVStore, t: Table, d: Doc, id: RowId): Unit
  def query(store: KVStore, t: Table, query: Query): List[RowId]
}

trait PointIndexer[Query] extends Indexer[Query] {
  def makeKey(t: Table, d: Doc): Try[String]
  def makeKey(t: Table, v: Query): String
  def insert(store: KVStore, t: Table, d: Doc, id: RowId) = makeKey(t, d).foreach { k =>
    store.append(k, id)
  }
  def query(store: KVStore, t: Table, query: Query): List[RowId] = {
    val rowids = store.get(makeKey(t, query)).asInstanceOf[Option[ListOfRowIds]]
    rowids.toList.flatMap(r => r.values)
  }

}

case class SimpleIndexer(name: String, project: Doc => Try[String]) extends PointIndexer[String] {
  def key(t: Table) = IndexKey(t.name.suffix("index").suffix(name))

  def makeKey(t: Table, d: Doc): Try[String] = project(d).map(pd => key(t).name.value.value(pd))
  def makeKey(t: Table, v: String): String = key(t).name.value.value(v)

}
case class CompoundIndexer(sub: Seq[SimpleIndexer]) extends PointIndexer[List[String]] {
  def key1(t: Table, name: String) = IndexKey(t.name.suffix("index").suffix(name))
  def key(t: Table): IndexKey = sub.drop(1).map(_.name).foldLeft(key1(t, sub.head.name))(_ compound _)
  def makeKey(t: Table, d: Doc) = Try(sub.map(i => i.project(d).get).mkString("_")).map(pd => key(t).name.value.value(pd))
  def makeKey(t: Table, v: List[String]) = key(t).name.value.value(v.mkString("_"))

}

trait TableStore { self: KVStore =>
  private def newId(t: Table): RowId = synchronized {
    get(t.lastId) match {
      case None => {
        val x = RowId(0L)
        put(t.lastId, x)
        x
      }
      case Some(RowId(l)) => {
        val x = RowId(l + 1)
        put(t.lastId, x)
        x
      }
    }
  }
  def insert(
    doc: Doc,
    t: Table,
    keys: Seq[Indexer[_]]
  ): RowId = {
    val id = newId(t)
    put(t.pk.value(id.toString), doc)
    keys.foreach { k =>
      k.insert(this, t, doc, id)

    }
    id
  }
  def get(r: RowId, t: Table): Option[Doc] = get(t.pk.value(r.id.toString)).asInstanceOf[Option[Doc]]
  def get[Q](idx: Indexer[Q], v: Q, t: Table): Seq[Doc] = {
    val rowids = idx.query(this, t, v)
    rowids.toList.flatMap(r => get(r, t).toList)
  }
  def indexAll(t: Table, keys: Seq[Indexer[_]]) = {
    listPrefix(t.pk.value(""))
      .filter(_._2.isInstanceOf[Doc]).foreach {
        case (key, doc) =>
          val id = RowId(key.drop(t.pk.value("").size).toLong)
          keys.foreach { k =>
            k.insert(this, t, doc.asInstanceOf[Doc], id)
          }
      }
  }
}
