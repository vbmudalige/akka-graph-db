import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import scala.concurrent.duration._


// Define Actor Messages
case class NodeCreator(name: String)

case class NodeUpdater(name: String)

// Define Greeter Actor
class MainActor extends Actor {
  def receive = {
    case NodeCreator(name) => println(s"Hello $name")
  }
}

object HelloAkkaScala extends App {

  // Create the actor system
  val system = ActorSystem("Akka-Graph")

  // Create the 'NodeCreator' actor
  val creator = system.actorOf(Props[NodeCreator], "creator")

  // Send Message to actor
  creator ! NodeCreator("Akka")

  //shutdown actorsystem
  system.terminate()

}
