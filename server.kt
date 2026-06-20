        MessageType.ADD_FRIEND -> {
            val user = msg.sender
            val target = msg.content ?: return
            
            // 1. Δες αν ο παίκτης υπάρχει στη βάση
            val targetExists = usersColl.find(Filters.eq("name", target)).firstOrNull()
            
            if (targetExists != null) {
                // 2. Δες αν είστε ήδη φίλοι
                val isFriend = friendsColl.find(Filters.and(Filters.eq("user", user), Filters.eq("friend", target))).firstOrNull()
                if (isFriend != null) {
                    session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Είναι ήδη φίλος σου!"))))
                    return
                }

                // 3. Γράψε το αίτημα στη MongoDB
                requestsColl.updateOne(
                    Filters.eq("target", target),
                    Updates.addToSet("requesters", user),
                    UpdateOptions().upsert(true)
                )

                // 4. ΕΙΔΟΠΟΙΗΣΕ ΤΟΝ ΦΙΛΟ ΣΟΥ ΑΜΕΣΩΣ (Αν είναι Online)
                val friendSession = onlineUsers[target]
                if (friendSession != null) {
                    sendRequestList(target, friendSession) // Του στέλνει τη νέα λίστα αιτημάτων
                    friendSession.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Νέο αίτημα φιλίας από $user!"))))
                }

                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Το αίτημα στάλθηκε!"))))
            } else {
                session.send(Frame.Text(gson.toJson(GameMessage(MessageType.ERROR, "Server", "Ο παίκτης $target δεν βρέθηκε!"))))
            }
        }
