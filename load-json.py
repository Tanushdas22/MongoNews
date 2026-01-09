#!/usr/bin/env python3
import sys
import json
from pymongo import MongoClient

BATCH_SIZE = 1000

def main():
    # Check command-line arguments - expect exactly two: filename and port
    if len(sys.argv) != 3:
        print("Usage: python3 load-json.py <json-file> <port>", file=sys.stderr)
        sys.exit(1)

    # Extract the filename and port string
    json_filename = sys.argv[1]
    port = sys.argv[2]

    # Convert the port string to an integer and handle a bad value
    try:
        port = int(port)
    except ValueError:
        print(f"Error: Port must be a number, got '{sys.argv[2]}'", file=sys.stderr)
        sys.exit(1)

    # Try to open the input file for reading with UTF-8 encoding
    try:
        file_handle = open(json_filename, 'r', encoding='utf-8')
    except FileNotFoundError:
        # If File Not Found, print an error and exit
        print(f"Error: File '{json_filename}' not found", file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        # General I/O errors
        print(f"Error: Cannot open file '{json_filename}': {e}", file=sys.stderr)
        sys.exit(1)

    # Connect to a local MongoDB server on the given port and verify the connection
    try:
        client = MongoClient('localhost', port)
        # A lightweight command to check the server is reachable
        client.admin.command('ping')
    except Exception as e:
        print(f"Error: Cannot connect to MongoDB on port {port}: {e}", file=sys.stderr)
        file_handle.close()
        sys.exit(1)

    # Use (or create) a database named '291db'
    db = client['291db']

    # If an 'articles' collection already exists, remove that collection and start fresh
    if 'articles' in db.list_collection_names():
        db['articles'].drop()
        print("Dropped existing 'articles' collection")

    collection = db['articles']

    # Prepare variables to hold the current batch, total documents inserted, and batch count
    batch = []
    total_documents = 0
    batch_count = 0

    try:
        # Read the file line-by-line
        for line_num, line in enumerate(file_handle, 1):
            # Remove leading/trailing whitespace including the newline
            line = line.strip()
            if not line:
                # Skip empty lines
                continue

            try:
                # Parse the JSON object on this line into a Python dict
                document = json.loads(line)

                # Basic validation to ensure required fields exist
                required_fields = ['id', 'content', 'title', 'media-type', 'source', 'published']
                if not all(field in document for field in required_fields):
                    # If a document is missing required fields, warn and skip it
                    print(f"Warning: Line {line_num} missing required fields, skipping", file=sys.stderr)
                    continue

                # Add the document to the batch
                batch.append(document)

                # If we've reached the batch size, write them to MongoDB
                if len(batch) >= BATCH_SIZE:
                    collection.insert_many(batch)
                    total_documents += len(batch)
                    batch_count += 1
                    print(f"Inserted batch {batch_count} ({len(batch)} documents)")
                    # Reset the batch list to collect the next group
                    batch = []

            except json.JSONDecodeError as e:
                # JSON parsing errors - e.g. bad inputs — warn and continue
                print(f"Warning: Invalid JSON on line {line_num}: {e}", file=sys.stderr)
                continue
            except Exception as e:
                # Catch for other unexpected per-line errors
                print(f"Warning: Error processing line {line_num}: {e}", file=sys.stderr)
                continue

        # After the loop, if any documents remain in the batch, insert them
        if batch:
            collection.insert_many(batch)
            total_documents += len(batch)
            batch_count += 1
            print(f"Inserted final batch {batch_count} ({len(batch)} documents)")

        print(f"\nCompleted! Total documents inserted: {total_documents}")

    except Exception as e:
        # If something goes wrong during the processing loop, report and exit
        print(f"Error during processing: {e}", file=sys.stderr)
        file_handle.close()
        client.close()
        sys.exit(1)
    finally:
        # Closing resources to avoid leaking file descriptors or DB connections
        file_handle.close()
        client.close()

if __name__ == "__main__":
    main()
