package indices

import java.io._
import java.nio.{ ByteBuffer, ByteOrder }

class SimpleLog(f: File) extends Log {
  val rf = new RandomAccessFile(f, "r")
  rf.seek(0)
  val magic = 0x983287ae
  val buf = Array.ofDim[Byte](8)
  val bufInt = Array.ofDim[Byte](4)

  assert(rf.length == 0 || readLong(rf, buf) == magic)

  if (rf.length == 0) {
    val rf = new RandomAccessFile(f, "rw")
    rf.seek(0)
    writeLong(magic, rf, buf)
    writeLong(16L, rf, buf)
    rf.close
  }

  var lastId = {
    rf.seek(8)
    readLong(rf, buf)
  }

  val os = new BufferedOutputStream(new FileOutputStream(f, true))

  def close = {
    rf.close
    os.close
    val rw = new RandomAccessFile(f, "rw")
    rw.seek(8)
    writeLong(lastId, rw, buf)
    rw.close
  }

  def append(d: Doc): RowId = {
    val bs = d.document.getBytes
    writeInt(bs.size, os, bufInt)
    os.write(bs)
    lastId += 4 + bs.size
    RowId(lastId)
  }

  def get(r: RowId) = {
    if (lastId < r.id) None
    else {
      rf.seek(r.id)
      val l = readInt(rf, bufInt)
      val a = Array.ofDim[Byte](l)
      rf.readFully(a)
      Some(Doc(new String(a)))
    }
  }

  def iterator = {
    val is = new BufferedInputStream(new FileInputStream(f))
    is.skip(16)
    new Iterator[(RowId, Doc)] {
      var pre: Option[(RowId, Doc)] = None
      var rowid = 16L
      def readahead = try {
        val l = readInt(is, bufInt)
        val ar = Array.ofDim[Byte](l)
        fill(is, buf)
        pre = Some((RowId(rowid), Doc(new String(ar))))
        rowid += l + 4

      } catch {
        case e: Exception => pre = None
      }

      readahead

      def hasNext = pre.isDefined
      def next = {
        val r = pre.get
        readahead
        r
      }
    }
  }

  def writeLong(l: Long, os: OutputStream, buf: Array[Byte]) = {
    val ar = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putLong(l)
    os.write(buf)
  }

  def writeLong(l: Long, os: RandomAccessFile, buf: Array[Byte]) = {
    val ar = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putLong(l)
    os.write(buf)
  }

  def readLong(raf: RandomAccessFile, buf: Array[Byte]) = {
    raf.readFully(buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong
  }

  def readInt(raf: RandomAccessFile, buf: Array[Byte]) = {
    raf.readFully(buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def readLong(is: InputStream, buf: Array[Byte]) = {
    fill(is, buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong
  }

  def readInt(is: InputStream, buf: Array[Byte]) = {
    fill(is, bufInt)
    ByteBuffer.wrap(bufInt).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def writeInt(l: Int, os: OutputStream, buf: Array[Byte]) = {
    val ar = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putInt(l)
    os.write(buf)
  }

  def fill(is: InputStream, ar: Array[Byte]) = {
    var len = ar.size
    val size = ar.size
    while (len > 0) {
      val c = is.read(ar, size - len, len)
      if (c < 0) throw new RuntimeException("unexpected end of stream")
      len -= c
    }
  }
}
