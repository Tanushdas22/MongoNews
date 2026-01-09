# MongoNews

Two-phase Python system for ingesting large JSON datasets of newspaper articles into MongoDB and enabling interactive querying, including top sources, recent articles, and word-frequency analysis by media type.  

**Tools:** Python, pymongo, MongoDB  

---

## System Overview

**NewsDB Explorer** is a two-phase system designed to manage and query large datasets of newspaper articles.  

- **Phase 1:** Loads a JSON file of articles into a MongoDB collection efficiently, with validation and batch insertion.  
- **Phase 2:** Provides an interactive menu to explore the database with queries such as most common words, article counts by date, top sources, and recent articles per source.  

This project was developed to gain experience with NoSQL databases, MongoDB aggregation pipelines, and Python-based data processing.  

---

## Setup

1. Ensure MongoDB is installed and running on your machine.  
2. Clone the repository:  
```bash
git clone <repo_url>
````

3. Place your JSON file with article records in the project directory.
4. Run Phase 1 to load data into MongoDB:

```bash
python load_json.py <your_json_file.json>
```

5. Run Phase 2 for interactive queries:

```bash
python phase2_query.py
```

---

## Phase 1 - Building the Document Store

1. Connects to a MongoDB server and creates a database `291db`.
2. Removes any existing `articles` collection to ensure a fresh load.
3. Processes the JSON file **line by line** to avoid memory issues with large datasets.
4. Inserts documents into MongoDB in **batches** for efficiency (e.g., 1,000 documents per batch).
5. Validates each document for required fields and handles errors (invalid JSON, missing fields, connection issues).
6. Reports the total number of documents successfully loaded.

---

## Phase 2 - Query Operations

Provides an interactive text-based menu with 5 options:

1. **Most common words by media type**

   * Enter `news` or `blog`.
   * Retrieves article content, tokenizes words (case-insensitive), counts frequencies, and displays the top five words (including ties at the fifth position).

2. **Article count comparison for a specific date**

   * Enter a date in `YYYY-MM-DD` format.
   * Displays the number of news and blog articles published on that date and which category had more.

3. **Top 5 news sources in 2015**

   * Shows sources with the highest publication counts in 2015, including ties for fifth place.

4. **Five most recent articles from a source**

   * Enter the name of a source.
   * Displays up to five most recent articles with titles and publication dates, including ties.

5. **Exit**

   * Quit the interactive menu.

Users can return to the menu to run additional queries.

---

## System Design Summary

**Phase 1 Design:**

* Rebuilds database from scratch for consistency.
* Processes JSON line by line to handle large datasets.
* Inserts documents in batches for efficiency.
* Validates documents and handles errors.
* Prints status messages for progress tracking.

**Phase 2 Design:**

* Simple text-based interface for clarity.
* Validates user inputs to prevent errors.
* Normalizes dates with `$toDate` for consistent comparisons.
* Uses MongoDB aggregation pipelines for efficient queries (top sources, tie-handling).
* Sanitizes user input before injection into pipelines to prevent malicious input.

---

## Assumptions and Decisions

* All publication timestamps follow ISO 8601 format (`YYYY-MM-DDTHH:MM:SSZ`).
* Media types are primarily `news` and `blog`; case-insensitive matching is applied.
* Each JSON object is one article per line.
* Source names are recognized regardless of capitalization or special characters.
* Fewer than five articles for a source: display all and include ties at the fifth position.
* Word-frequency feature counts all words from article content; stopwords are included.

---

## Usage Example

**Load JSON into MongoDB:**

```bash
python load_json.py articles.json
```

**Run interactive query menu:**

```bash
python phase2_query.py
```

Sample menu interaction:

```
Select an option:
1. Most common words by media type
2. Article count comparison for a specific date
3. Top 5 news sources in 2015
4. Five most recent articles from a source
5. Exit
```

---

## References

* MongoDB Aggregation Framework: [https://www.mongodb.com/docs/manual/aggregation/](https://www.mongodb.com/docs/manual/aggregation/)
* PyMongo Documentation: [https://pymongo.readthedocs.io/en/stable/](https://pymongo.readthedocs.io/en/stable/)

```
