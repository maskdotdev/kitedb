import { kite, node, edge, prop, optional } from "@kitedb/core";

// Define your schema
const User = node("user", {
  key: (id: string) => `user:${id}`,
  props: {
    name: prop.string("name"),
    email: prop.string("email"),
    age: optional(prop.int("age")),
  },
});

const Knows = edge("knows", {
  since: prop.int("since"),
});

// Open database (async)
const db = await kite("./social.kitedb", {
  nodes: [User],
  edges: [Knows],
});

// Insert nodes
const alice = db
  .insert(User)
  .values({ key: "alice", name: "Alice", email: "alice@example.com" })
  .returning();
const bob = db
  .insert(User)
  .values({ key: "bob", name: "Bob", email: "bob@example.com" })
  .returning();

// Create edges
db.link(alice, Knows, bob, { since: 2024 });

// Traverse
const friends = db.from(alice).out(Knows).toArray();

// Pathfinding
const path = db.shortestPath(alice).via(Knows).to(bob).dijkstra();

db.close();
