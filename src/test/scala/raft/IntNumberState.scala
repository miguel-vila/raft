package raft

trait NumberCommand
case class Add(n: Int) extends NumberCommand
case class Minus(n: Int) extends NumberCommand

class IntNumberState extends StateMachine[NumberCommand, Int](0) {

  def applyCommand(command: NumberCommand, currentState: Int): Int = command match {
    case Add(n) => currentState + n
    case Minus(n) => currentState - n
  }

}
