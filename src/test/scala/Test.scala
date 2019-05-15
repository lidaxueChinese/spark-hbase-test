import com.bitnei.DateUtil

/**
  * Created by lidaxue@bitnei.cn on 2019/5/13.
  */
class Test {



}

object Test{
  def main(args:Array[String]): Unit ={
    val timestamp = DateUtil.getFilterDatePrefix("20190511")
    println(timestamp)
  }
}
