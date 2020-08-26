import com.batch.LatestPartition
import com.common.conf.{Init, SparkConfiguration}
import org.scalatest.FunSuite

class LatestPartitionTest extends FunSuite{
  new SparkConfiguration()
  new Init()
  val latestpartition = new  LatestPartition()
  test("ReadLatestPartitions.getLatestPartition") {
    val filePath = "/path"
    val output ="/outputpath"
    assert(latestpartition.getLatestPartition(filePath) === output)
  }
  test("ReadLatestPartitions.checkPartition") {
    val output ="/outputresultpath"
    assert(latestpartition.getFilesCount(output) === 1)
  }
}

