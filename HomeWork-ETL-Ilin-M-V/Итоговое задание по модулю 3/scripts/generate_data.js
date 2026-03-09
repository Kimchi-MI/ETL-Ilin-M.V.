db = db.getSiblingDB('myapp_db');

db.user_sessions.drop();
db.support_tickets.drop();

for (let user = 1; user <= 100; user++) {
    let sessions = [];
    for (let session = 1; session <= 10; session++) {
        let start = new Date();
        start.setDate(start.getDate() - Math.floor(Math.random() * 30));
        let end = new Date(start);
        end.setMinutes(end.getMinutes() + Math.floor(Math.random() * 60) + 5);
        
        sessions.push({
            session_id: "sess_" + user + "_" + session,
            user_id: "user_" + user,
            start_time: start.toISOString(),
            end_time: end.toISOString(),
            pages_visited: ["/home", "/products", "/cart", "/checkout"].slice(0, Math.floor(Math.random() * 4) + 1),
            device: ["mobile", "desktop", "tablet"][Math.floor(Math.random() * 3)],
            actions: ["login", "view_product", "add_to_cart", "logout"].slice(0, Math.floor(Math.random() * 4) + 1)
        });
    }
    db.user_sessions.insertMany(sessions);
}

let tickets = [];
let issueTypes = ["payment", "technical", "delivery", "product_info", "other"];
let statuses = ["open", "in_progress", "closed"];

for (let i = 1; i <= 150; i++) {
    let created = new Date();
    created.setDate(created.getDate() - Math.floor(Math.random() * 20));
    let updated = new Date(created);
    if (Math.random() > 0.3) {
        updated.setHours(updated.getHours() + Math.floor(Math.random() * 48));
    }
    
    tickets.push({
        ticket_id: "ticket_" + String(i).padStart(3, '0'),
        user_id: "user_" + Math.floor(Math.random() * 100 + 1),
        status: statuses[Math.floor(Math.random() * 3)],
        issue_type: issueTypes[Math.floor(Math.random() * 5)],
        messages: [{
            sender: "user",
            message: "Sample message",
            timestamp: created.toISOString()
        }],
        created_at: created.toISOString(),
        updated_at: updated.toISOString()
    });
    
}
db.support_tickets.insertMany(tickets);

