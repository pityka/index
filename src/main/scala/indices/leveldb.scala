package indices

import scala.collection.JavaConversions._
import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

class LevelDBKVStore(file: java.io.File) extends {

  private val leveldb = {
    if (file.isFile) {
      throw new RuntimeException("File exists: " + file.getAbsolutePath)
    } else if (!file.isDirectory) {
      file.mkdirs
    }
    val options = new Options();
    options.createIfMissing(true);
    options.maxOpenFiles(50);
    factory.open(file, options);
  }

  private val writeOption = {
    val o = new WriteOptions
    o.sync(false)
  }

} with KVStore {
  def read(s: String) = s(0) match {
    case 'A' => RowId(s.drop(1).toLong)
  }

  def write(v: Value) = v match {
    case RowId(l) => "A" + l
  }

  def get(k: String) = {
    val r = leveldb.get(k.getBytes)
    if (r == null) None else Some(read(new String(r)))
  }
  def prefix(k: String): Iterator[(String, Value)] = {
    val iter = leveldb.iterator
    iter.seek(k.getBytes)
    iter.map(x => new String(x.getKey) -> read(new String(x.getValue))).takeWhile(_._1.startsWith(k))
  }
  def put(k: String, v: Value): Unit = leveldb.put(k.getBytes, write(v).getBytes)
  def put(v: Seq[(String, Value)]): Unit = {
    val b = leveldb.createWriteBatch
    v.foreach {
      case (k, v) =>
        b.put(k.getBytes, write(v).getBytes)
    }
    leveldb.write(b)
  }
  def close: Unit = leveldb.close
}
