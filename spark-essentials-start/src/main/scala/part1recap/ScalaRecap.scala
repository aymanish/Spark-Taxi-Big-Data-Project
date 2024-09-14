package part1recap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  // values and variables
  val aBoolean: Boolean = false

  // expressions:
  val anExpression = if(2 > 3) "bigger" else "smaller"

  // instructions vs expressions
 // instructions are unit types
 val theUnit = println(("Hello Scala"))

 // funcitons
 def myFunction(x: Int) = 42

 //OOP
 class Animal
 class Dog extends Animal

 // interface
 trait Carnivore {
   // abstract methods
   def eat(animal: Animal): Unit
 }

 class Crocodile extends Animal with Carnivore {
   override def eat(animal: Animal): Unit = println("Crunch")
 }

 // singleton object
 object MySingleton // type Singleton and instance Singleton

 // companions:
 object Carnivore

 // generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementer: (Int) => Int = new Function[Int, Int] {
    override def apply(v1: Int): Int = x + 1
  }
  val incremented = incrementer(42)
  // map. flatmap, filter
  val processedList = List(1,2,3).map(incrementer)

  // pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try - catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // Future
  val aFuture = Future {
    // some expensive computation
    42
  }


  aFuture.onComplete {
    case Success(meaning0fLife) => println(s"Ive found the meaning of life: $meaning0fLife")
    case Failure(exception) => println(s"I have failed: $exception")
  }

  // Partial function
  // func that takes in an int and match cases x
  // whole structure can be reduced to a partial function
  // between an int and int
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits
  // auto injection by the compiler
  def methodImplicitArg(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  val implicitCall = methodImplicitArg // implicittarg(67)

  // implicit conversions
  // implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi my name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  "Bob".greet // fromStringtoPerson("Bob").greet
  // connects bob string to person case class and uses the from string to person and
  // uses person method greet

  // implicit classes
  implicit class implicitDog(name: String) {
    def bark = println(s"$name Barks!")
  }
  "Lassie".bark // auto creates dog from lassie string and runs bark method on newly created dog
  

}
