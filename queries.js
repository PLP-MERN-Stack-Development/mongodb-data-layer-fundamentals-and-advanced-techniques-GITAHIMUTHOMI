// queries.js
const { MongoClient } = require("mongodb");
const uri = "mongodb://127.0.0.1:27017";  //‑‑ change this if you use Atlas
const client = new MongoClient(uri);

async function runQueries() {
  try {
    await client.connect();
    const db = client.db("plp_bookstore");
    const books = db.collection("books");

    // --- Task 2 CRUD operations --‑‑

    // 1. Find all books in a specific genre
    console.log("Books in genre Fiction:");
    const fictionBooks = await books.find({ genre: "Fiction" }).toArray();
    console.log(fictionBooks);

    // 2. Find books published after a certain year
    console.log("Books published after 2010:");
    const recentBooks = await books.find({ published_year: { $gt: 2010 } }).toArray();
    console.log(recentBooks);

    // 3. Find books by a specific author
    console.log("Books by Author A:");
    const byAuthorA = await books.find({ author: "Author A" }).toArray();
    console.log(byAuthorA);

    // 4. Update the price of a specific book
    console.log("Updating price of Book One:");
    const updateResult = await books.updateOne(
      { title: "Book One" },
      { $set: { price: 22.99 } }
    );
    console.log("Modified count:", updateResult.modifiedCount);

    // 5. Delete a book by its title
    console.log("Deleting Book Two:");
    const deleteResult = await books.deleteOne({ title: "Book Two" });
    console.log("Deleted count:", deleteResult.deletedCount);

    // --- Task 3 Advanced queries --‑‑

    // Query: books that are both in stock AND published after 2010, projection only title, author, price
    console.log("Books in stock & published after 2010 (title, author, price):");
    const filteredBooks = await books
      .find(
        { in_stock: true, published_year: { $gt: 2010 } },
        { projection: { _id: 0, title: 1, author: 1, price: 1 } }
      )
      .toArray();
    console.log(filteredBooks);

    // Sort by price ascending
    console.log("Books sorted by price (ascending):");
    const sortedAsc = await books.find().sort({ price: 1 }).toArray();
    console.log(sortedAsc);

    // Sort by price descending
    console.log("Books sorted by price (descending):");
    const sortedDesc = await books.find().sort({ price: -1 }).toArray();
    console.log(sortedDesc);

    // Pagination: show page 2 (skip first 5, limit 5)
    console.log("Page 2 (books 6‑10):");
    const page2 = await books.find().skip(5).limit(5).toArray();
    console.log(page2);

    // --- Task 4 Aggregation pipelines --‑‑

    // Average price of books by genre
    console.log("Average price by genre:");
    const avgByGenre = await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
    ]).toArray();
    console.log(avgByGenre);

    // Author with the most books
    console.log("Author with most books:");
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log(topAuthor);

    // Group books by publication decade and count them
    console.log("Books grouped by decade:");
    const byDecade = await books.aggregate([
      {
        $project: {
          decade: { $multiply: [ { $floor: { $divide: [ "$published_year", 10 ] } }, 10 ] }
        }
      },
      { $group: { _id: "$decade", count: { $sum: 1 } } },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log(byDecade);

    // --- Task 5 Indexing demonstration (example queries with explain) --‑‑

    console.log("Creating index on title:");
    await books.createIndex({ title: 1 });

    console.log("Creating compound index on author + published_year:");
    await books.createIndex({ author: 1, published_year: 1 });

    console.log("Explain for a query using index on title:");
    const explainTitle = await books.find({ title: "Book One" }).explain("executionStats");
    console.log(explainTitle.executionStats);

    console.log("Explain for a query using index on author + published_year:");
    const explainCompound = await books.find({ author: "Author A", published_year: 2015 }).explain("executionStats");
    console.log(explainCompound.executionStats);

  } finally {
    await client.close();
  }
}

runQueries().catch(console.dir);
