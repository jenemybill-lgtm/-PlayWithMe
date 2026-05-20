import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import io.ktor.server.application.*
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.*

enum class MessageType { CREATE_ROOM, JOIN, JOIN_RESPONSE, START_GAME, QUESTION, ANSWER, RESULT, LEADERBOARD, GAME_OVER, ERROR }
data class GameMessage(val type: MessageType, val sender: String, val content: String? = null)
data class Player(val name: String, val session: DefaultWebSocketServerSession, var score: Int = 0)

class GameRoom(val code: String, val hostSession: DefaultWebSocketServerSession) {
    val players = mutableListOf<Player>()
    var questions: List<Any> = emptyList()
    var currentQuestionIndex = 0
    
    suspend fun broadcast(message: GameMessage) {
        val text = Gson().toJson(message)
        try { hostSession.send(Frame.Text(text)) } catch(e: Exception) {}
        players.forEach { try { it.session.send(Frame.Text(text)) } catch(e: Exception) {} }
    }
}

val rooms = ConcurrentHashMap<String, GameRoom>()

fun main() {
    val port = System.getenv("PORT")?.toInt() ?: 8080
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        install(WebSockets)
        routing {
            webSocket("/ws") {
                try {
                    for (frame in incoming) {
                        if (frame is Frame.Text) {
                            val text = frame.readText()
                            val msg = Gson().fromJson(text, GameMessage::class.java)
                            handleMessage(this, msg)
                        }
                    }
                } catch (e: Exception) { e.printStackTrace() }
            }
        }
    }.start(wait = true)
}

suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    val gson = Gson()
    when (msg.type) {
        MessageType.CREATE_ROOM -> {
            val code = (1000..9999).random().toString()
            rooms[code] = GameRoom(code, session)
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.JOIN_RESPONSE, "Server", code))))
        }
        MessageType.JOIN -> {
            val room = rooms[msg.content]
            if (room != null) {
                room.players.add(Player(msg.sender, session))
                room.broadcast(GameMessage(MessageType.JOIN_RESPONSE, "Server", "Player ${msg.sender} joined"))
            } else {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Room not found"))))
            }
        }
        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null && msg.content != null) {
                // Λήψη ερωτήσεων από τον Host
                val listType = object : TypeToken<List<Any>>() {}.type
                room.questions = gson.fromJson(msg.content, listType)
                room.currentQuestionIndex = 0
                
                // Ενημέρωση όλων να ξεκινήσουν
                room.broadcast(GameMessage(MessageType.START_GAME, "Server", "Game Started!"))
                
                // Αναμονή 3 δευτερόλεπτα και αποστολή 1ης ερώτησης
                CoroutineScope(Dispatchers.Default).launch {
                    delay(3000)
                    sendNextQuestion(room)
                }
            }
        }
        MessageType.ANSWER -> {
            // Εδώ μπορείς αργότερα να προσθέσεις πόντους
        }
        else -> {}
    }
}

suspend fun sendNextQuestion(room: GameRoom) {
    if (room.currentQuestionIndex < room.questions.size) {
        val question = room.questions[room.currentQuestionIndex]
        room.broadcast(GameMessage(MessageType.QUESTION, "Server", Gson().toJson(question)))
        room.currentQuestionIndex++
    } else {
        room.broadcast(GameMessage(MessageType.GAME_OVER, "Server", "Game Over!"))
    }
}
