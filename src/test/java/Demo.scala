import java.util.Optional

case class Demo(id:String,name:String)
{

}

object Demo{

  def unApply(demo: Demo): Option[(String,String)] ={
    Some(demo.id,demo.name)
  }

}