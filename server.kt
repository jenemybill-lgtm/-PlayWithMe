import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import com.google.gson.Gson
import java.util.*
import java.util.concurrent.ConcurrentHashMap

// Τα ίδια μοντέλα που έχουμε στο Android
enum class MessageType { CREATE_ROOM, JOIN, JOIN_RESPONSE, START_GAME, QUESTION, ANSWER, RESULT, LEADERBOARD, GAME_OVER, ERROR }
data class GameMessage(val type: MessageType, val sender: String, val content: String? = null)
data class Player(val name: String, val session: DefaultWebSocketServerSession, var score: Int = 0)

class GameRoom(val code: String, val hostSession: DefaultWebSocketServerSession) {
    val players = mutableListOf<Player>()
    var questionsJson: String? = null
    
    suspend fun broadcast(message: GameMessage) {
        val text = Gson().toJson(message)
        hostSession.send(text)
        players.forEach { it.session.send(text) }
    }
}

val rooms = ConcurrentHashMap<String, GameRoom>()

fun main() {
    embeddedServer(Netty, port = System.getenv("PORT")?.toInt() ?: 8080) {
        install(WebSockets)
        routing {
            webSocket("/ws") {
                try {
                    for (frame in incoming) {
                        if (frame is Frame.Text) {
                            val msg = Gson().fromJson(frame.readText(), GameMessage::class.java)
                            handleMessage(this, msg)
                        }
                    }
                } finally {
                    // Καθαρισμός όταν κάποιος αποσυνδέεται
                }
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
            session.send(gson.toJson(GameMessage(MessageType.JOIN_RESPONSE, "Server", code)))
        }
        MessageType.JOIN -> {
            val room = rooms[msg.content] // Το content έχει τον κωδικό δωματίου
            if (room != null) {
                room.players.add(Player(msg.sender, session))
                room.broadcast(GameMessage(MessageType.JOIN_RESPONSE, "Server", "Player ${msg.sender} joined room ${room.code}"))
            } else {
                session.send(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Room not found")))
            }
        }
        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null) {
                room.questionsJson = msg.content // Ο Host στέλνει τις ερωτήσεις εδώ
                room.broadcast(GameMessage(MessageType.START_GAME, "Server", "Game Started!"))
            }
        }
        MessageType.ANSWER -> {
            // Εδώ προσθέτεις τη λογική για το Leaderboard
            val room = rooms.values.find { r -> r.players.any { it.session == session } || r.hostSession == session }
            room?.broadcast(msg) // Προώθηση της απάντησης για το real-time leaderboard
        }
        else -> {}
    }
}
