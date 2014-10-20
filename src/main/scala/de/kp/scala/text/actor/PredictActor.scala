package de.kp.scala.text.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Scala-Text project
* (https://github.com/skrusche63/scala-text).
* 
* Scala-Text is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Scala-Text is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Scala-Text. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import akka.actor.{Actor,ActorLogging}

import de.kp.scala.text.model._

class PredictActor  extends Actor with ActorLogging {
 
  def receive = {

    case req:ServiceRequest => {
      /*
       * Not implemented yet
       */
    }
    
  }

}