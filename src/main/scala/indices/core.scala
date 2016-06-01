package indices

import scala.util.Try

sealed trait Value

case class RowId(id: Long) extends Value { override def toString = id.toString }
case class Doc(document: String) extends Value

trait KVStore {
  def get(k: String): Option[Value]
  def prefix(k: String): Iterator[(String, Value)]
  def put(k: String, v: Value): Unit
  def put(v: Seq[(String, Value)]): Unit
  def closedb: Unit
}

case class NameSpace(segments: Vector[String]) {
  def suffix(value: String) = NameSpace(segments :+ value)
  def apply(value: String) = suffix(value).segments.mkString("_")
}
object Keys {
  val root = NameSpace(Vector())
  def tables(s: String) = root.suffix("table").suffix(s)
  val lastId = "__lastid__"
}

case class Table(name: NameSpace) {
  val pk: NameSpace = name.suffix("pk")
}

trait Indexer[-Query] {

  def insert(store: KVStore, t: Table, d: Doc, id: RowId): Unit
  def query(store: KVStore, t: Table, query: Query): List[RowId]
}

trait PointIndexer[Query] extends Indexer[Query] {

  def insertKey(t: Table, d: Doc): Try[String]
  def queryKey(t: Table, v: Query): String

  def insert(store: KVStore, t: Table, d: Doc, id: RowId) = insertKey(t, d).foreach { k =>
    store.put(k + "!" + id.id, id)
  }
  def query(store: KVStore, t: Table, query: Query): List[RowId] = {
    store.prefix(queryKey(t, query)).map(_._2.asInstanceOf[RowId]).toList
  }

}

case class SimpleIndexer(name: String, project: Doc => Try[String]) extends PointIndexer[String] {
  def key(t: Table) = t.name.suffix("index").suffix(name)

  def insertKey(t: Table, d: Doc): Try[String] = project(d).map(pd => key(t)(pd))
  def queryKey(t: Table, v: String): String = key(t)(v)

}
case class CompoundIndexer(sub: Seq[SimpleIndexer]) extends PointIndexer[List[String]] {
  def key(t: Table) = t.name.suffix("compound").suffix(sub.map(_.name).mkString("_"))
  def insertKey(t: Table, d: Doc) = Try(sub.map(i => i.project(d).get).mkString("_")).map(pd => key(t)(pd))
  def queryKey(t: Table, v: List[String]) = key(t)(v.mkString("_"))

}

trait TableStore { self: KVStore =>

  var lastId: Long = get(Keys.lastId) match {
    case None => -1L
    case Some(RowId(l)) => l
  }

  def close = {
    put(Keys.lastId, RowId(lastId));
    closedb
  }

  private def newId(t: Table): Long = {
    lastId += 1
    lastId
  }

  def insert(
    doc: Seq[Doc],
    t: Table,
    keys: Seq[Indexer[_]]
  ): Seq[RowId] = {

    val (rowids, pairs) = doc.map { doc =>
      val id = newId(t)

      keys.filterNot(_.isInstanceOf[PointIndexer[_]]).foreach { k =>
        k.insert(this, t, doc, RowId(id))
      }

      val indexPairs = keys.filter(_.isInstanceOf[PointIndexer[_]]).flatMap { k =>
        k.asInstanceOf[PointIndexer[_]].insertKey(t, doc).toOption.toList.map { key =>
          (key + "!" + id, RowId(id))
        }
      }

      (RowId(id), Seq(t.pk(id.toString) -> doc) ++ indexPairs)
    }.unzip

    put(pairs.flatten)

    rowids
  }

  def insert(
    doc: Doc,
    t: Table,
    keys: Seq[Indexer[_]]
  ): RowId = insert(List(doc), t, keys).head

  def get(r: RowId, t: Table): Option[Doc] = get(t.pk(r.id.toString)).asInstanceOf[Option[Doc]]
  def get[Q](idx: Indexer[Q], v: Q, t: Table): Seq[Doc] = {
    val rowids = idx.query(this, t, v)
    rowids.toList.flatMap(r => get(r, t).toList)
  }
}
