package raft

abstract class StateMachine[A, B](initialState: B) {

  var state = initialState

  def applyCommand(command: A, currentState: B): B

  def execute(command: A): Unit = {
    state = applyCommand(command, state)
  }

}
