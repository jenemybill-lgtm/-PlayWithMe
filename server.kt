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

enum class MessageType { CREATE_ROOM, JOIN, JOIN_RESPONSE, START_GAME, QUESTION, ANSWER, RESULT, LEADERBOARD, GAME_OVER, ERROR, RESTART }
data class GameMessage(val type: MessageType, val sender: String, val content: String? = null)
data class Player(val name: String, val session: DefaultWebSocketServerSession, var score: Int = 0, var hasAnswered: Boolean = false, var lastAnswerIndex: Int = -1, var isEliminated: Boolean = false)

class GameRoom(val code: String, val hostSession: DefaultWebSocketServerSession) {
    val players = mutableListOf<Player>()
    var questions: List<Map<String, Any>> = emptyList()
    var extraQuestions: List<Map<String, Any>> = emptyList()
    var currentQuestionIndex = 0
    var timerSeconds = 20
    var waitingForAnswers = false
    var isSuddenDeath = false
    
    suspend fun broadcast(message: GameMessage) {
        val text = Gson().toJson(message)
        val uniqueSessions = players.map { it.session }.toMutableSet()
        uniqueSessions.add(hostSession)
        uniqueSessions.forEach { try { it.send(Frame.Text(text)) } catch(e: Exception) {} }
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
                rooms.values.removeIf { it.hostSession == this }
                rooms.values.forEach { it.players.removeIf { p -> p.session == this } }
            }
        }
    }.start(wait = true)
}

suspend fun handleMessage(session: DefaultWebSocketServerSession, msg: GameMessage) {
    val gson = Gson()
    when (msg.type) {
        MessageType.CREATE_ROOM -> {
            val code = (1000..9999).random().toString()
            val room = GameRoom(code, session)
            room.players.add(Player(msg.sender, session))
            rooms[code] = room
            session.send(Frame.Text(gson.toJson(GameMessage(MessageType.JOIN_RESPONSE, "Server", code))))
        }
        MessageType.JOIN -> {
            val room = rooms[msg.content]
            if (room != null) {
                if (room.players.none { it.session == session }) room.players.add(Player(msg.sender, session))
                room.broadcast(GameMessage(MessageType.JOIN_RESPONSE, "Server", room.code))
            }
        }
        MessageType.START_GAME -> {
            val room = rooms.values.find { it.hostSession == session }
            if (room != null && msg.content != null) {
                val setup: Map<String, Any> = gson.fromJson(msg.content, object : TypeToken<Map<String, Any>>() {}.type)
                room.questions = setup["questions"] as List<Map<String, Any>>
                room.extraQuestions = setup["extra"] as List<Map<String, Any>>
                room.timerSeconds = (setup["timer"] as Double).toInt()
                room.currentQuestionIndex = 0
                room.isSuddenDeath = false
                room.players.forEach { it.score = 0; it.isEliminated = false; it.hasAnswered = false }
                room.broadcast(GameMessage(MessageType.START_GAME, "Server", room.timerSeconds.toString()))
                CoroutineScope(Dispatchers.Default).launch { delay(2000); runGameLoop(room) }
            }
        }
        MessageType.ANSWER -> {
            val room = rooms.values.find { r -> r.players.any { it.session == session } }
            val player = room?.players?.find { it.session == session }
            if (room != null && room.waitingForAnswers && player != null && !player.hasAnswered) {
                player.hasAnswered = true
                player.lastAnswerIndex = msg.content?.toIntOrNull() ?: -1
                val activePlayers = room.players.filter { !it.isEliminated }
                if (activePlayers.all { it.hasAnswered }) room.waitingForAnswers = false 
            }
        }
        else -> {}
    }
}

suspend fun runGameLoop(room: GameRoom) {
    while (room.currentQuestionIndex < room.questions.size) {
        sendQuestionAndWait(room, room.questions[room.currentQuestionIndex])
        room.currentQuestionIndex++
    }
    
    val sorted = room.players.sortedByDescending { it.score }
    if (sorted.size >= 2 && sorted[0].score == sorted[1].score && sorted[0].score > 0) {
        room.isSuddenDeath = true
        room.broadcast(GameMessage(MessageType.RESULT, "Server", "ΙΣΟΠΑΛΙΑ! SUDDEN DEATH!"))
        delay(4000)
        val topScore = sorted[0].score
        room.players.forEach { if (it.score < topScore) it.isEliminated = true }
        var sdIdx = 0
        while (room.players.count { !it.isEliminated } > 1 && sdIdx < room.extraQuestions.size) {
            val q = room.extraQuestions[sdIdx]
            sendQuestionAndWait(room, q)
            val correctIdx = (q["correctAnswerIndex"] as Double).toInt()
            room.players.filter { !it.isEliminated }.forEach { if (it.lastAnswerIndex != correctIdx) it.isEliminated = true }
            if (room.players.count { !it.isEliminated } == 0) room.players.forEach { if (it.score == topScore) it.isEliminated = false }
            sdIdx++
        }
    }

    val finalRank = room.players.sortedByDescending { it.score }
    val winner = finalRank.firstOrNull()
    val rankingText = finalRank.withIndex().joinToString("\n") { "${it.index + 1}. ${it.value.name}: ${it.value.score}" }
    
    room.players.forEach { p ->
        val resultMsg = if (p.name == winner?.name) "Χάρηκες ??? Δεν νίκησες και τον Τέσλα!" else "Έχασες από αυτόν ?? (${winner?.name})"
        try { p.session.send(Frame.Text(Gson().toJson(GameMessage(MessageType.GAME_OVER, "Server", "$resultMsg\n\nΚΑΤΑΤΑΞΗ:\n$rankingText")))) } catch(e: Exception) {}
    }
}

suspend fun sendQuestionAndWait(room: GameRoom, question: Map<String, Any>) {
    val gson = Gson()
    val correctIdx = (question["correctAnswerIndex"] as Double).toInt()
    room.players.forEach { it.hasAnswered = false; it.lastAnswerIndex = -1 }
    room.waitingForAnswers = true
    room.broadcast(GameMessage(MessageType.QUESTION, "Server", gson.toJson(question)))
    var elapsed = 0
    while (elapsed < room.timerSeconds && room.waitingForAnswers) { delay(1000); elapsed++ }
    room.waitingForAnswers = false
    if (!room.isSuddenDeath) room.players.forEach { if (it.lastAnswerIndex == correctIdx) it.score += 10 }
    val options = question["options"] as List<String>
    room.broadcast(GameMessage(MessageType.RESULT, "Server", "Σωστή απάντηση: ${options[correctIdx]}"))
    delay(4000)
    val lb = room.players.sortedByDescending { it.score }.joinToString("\n") { "${it.name}: ${it.score}" }
    room.broadcast(GameMessage(MessageType.LEADERBOARD, "Server", "Σκορ:\n$lb"))
    delay(3000)
}
