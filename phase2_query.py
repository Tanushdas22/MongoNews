#!/usr/bin/env python3
import sys
from datetime import datetime
from pymongo import MongoClient


def print_menu():
    # Show the user Menu to pick a query to run
    print("\n Mini-P2 Phase 2 Menu (prepared by project npm_run_dev)")
    print("-----------------------------------------------------")
    print("1. Most common words by media type")
    print("2. Article count difference between news and blogs")
    print("3. Top 5 News Sources by Article Count (2015)")
    print("4. 5 Most Recent Articles by Source")
    print("5. Exit")

# Handle option 1: Most common words by media type
def handle_common_words(collection):
    media_type = input("Enter media type (news/blog): ").strip().lower()
    
    if media_type not in ['news', 'blog']:
        print("Entered media type was invalid. Please enter 'news' or 'blog' only.")
        return
    
    # Use aggregation pipeline to do all processing in MongoDB.
    # This pipeline does: filter, then lowercase, then tokenize, then count, then sort
    pipeline = [
        # 1) Filter by media type, case-insensitive
        {
            '$match': {
                '$expr': {
                    '$eq': [
                        {'$toLower': '$media-type'},
                        media_type
                    ]
                }
            }
        },
        # 2) Build a lowercase text field from content only
        {
            '$project': {
                'text': {
                    '$toLower': {
                        '$ifNull': ['$content', '']
                    }
                }
            }
        },
        # 3) Extract word tokens using regexFindAll
        {
            '$project': {
                'words': {
                    '$map': {
                        'input': {
                            '$regexFindAll': {
                                'input': '$text',
                                'regex': '[a-zA-Z0-9_-]+'
                            }
                        },
                        'as': 'm',
                        'in': '$$m.match'
                    }
                }
            }
        },
        # 4) Unwind the words array; create one document per word
        {'$unwind': '$words'},
        # 5) Group by word and count
        {
            '$group': {
                '_id': '$words',
                'count': {'$sum': 1}
            }
        },
        # 6) Sort by count descending, then word ascending for result order
        {'$sort': {'count': -1, '_id': 1}}
    ]
    
    # Get all results
    all_results = list(collection.aggregate(pipeline))
    
    if not all_results:
        print(f"No articles found for media type '{media_type}' or no content available in the database.")
        return
    
    # Get top 5, then include any words that tie with the 5th place
    top_5 = all_results[:5]
    
    if len(top_5) >= 5:
        fifth_count = top_5[4]['count']
        # Track words already included in top 4 (to avoid duplicates)
        included_words = {item['_id'] for item in top_5[:4]}
        # Find all words with the same count as the 5th item, excluding those already in top 4
        tied_words = [
            item for item in all_results 
            if item['count'] == fifth_count and item['_id'] not in included_words
        ]
        # Combine top 4 with all tied at 5th position (deduplicated)
        result = top_5[:4] + tied_words
    else:
        result = top_5
    
    # Print results
    print(f"\nTop 5 most common words for '{media_type}'are:")
    for i, item in enumerate(result, 1):
        word = item['_id']
        count = item['count']
        print(f"{i}. {word}: {count}")

# Handle option 2: Article count difference between news and blogs
def handle_article_count(collection):
    date_str = input("Enter date (YYYY-MM-DD, e.g., 2015-09-01): ").strip()
    
    # Validate date
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print("Date format was invalid. Please use YYYY-MM-DD format.")
        return
    
    # Use aggregation pipeline to count articles by media type for the given date
    # We normalize the 'published' field to a date and build a date string like 'YYYY-MM-DD' so we can match exactly on the day
    pipeline = [
        # 1) Compute date-only string and lowercase media type
        {
            '$project': {
                'mediaTypeLower': {'$toLower': '$media-type'},
                'dateOnly': {
                    '$dateToString': {
                        'format': '%Y-%m-%d',
                        'date': {
                            '$cond': {
                                'if': {'$eq': [{'$type': '$published'}, 'date']},
                                'then': '$published',
                                'else': {'$toDate': '$published'}
                            }
                        }
                    }
                }
            }
        },
        # 2) Match only docs for that date and where media type is news/blog
        {
            '$match': {
                'dateOnly': date_str,
                'mediaTypeLower': {'$in': ['news', 'blog']}
            }
        },
        # 3) Group by media type and count
        {
            '$group': {
                '_id': '$mediaTypeLower',
                'count': {'$sum': 1}
            }
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    
    if not results:
        print("No articles were published on this day.")
        return
    
    # Map counts to variables for easy comparison
    news_count = 0
    blog_count = 0
    
    # Loop through results and assign counts
    for item in results:
        media_type = item['_id']
        count = item['count']
        if media_type == 'news':
            news_count = count
        elif media_type == 'blog':
            blog_count = count
    
    # Print results
    print(f"\nArticles published on {date_str}:")
    print(f"News articles: {news_count}")
    print(f"Blog articles: {blog_count}")
    
    if news_count > blog_count:
        diff = news_count - blog_count
        print(f"News had more articles by {diff}")
    elif blog_count > news_count:
        diff = blog_count - news_count
        print(f"Blogs had more articles by {diff}")
    else:
        print("Both media types had the same number of articles.")

# Handle option 3: Top sources for 2015
def handle_top_sources_2015(collection):
    # Use aggregation pipeline with $year to filter 2015 and then count by source
    pipeline = [
        # 1) Filter to year 2015 using $year
        {
            '$match': {
                '$expr': {
                    '$eq': [
                        {
                            '$year': {
                                '$cond': {
                                    'if': {'$eq': [{'$type': '$published'}, 'date']},
                                    'then': '$published',
                                    'else': {'$toDate': '$published'}
                                }
                            }
                        },
                        2015
                    ]
                }
            }
        },
        # 2) Group by source and count
        {
            '$group': {
                '_id': '$source',
                'count': {'$sum': 1}
            }
        },
        # 3) Sort by count descending, then source ascending for deterministic ordering
        {'$sort': {'count': -1, '_id': 1}}
    ]
    
    # Get all results
    all_results = list(collection.aggregate(pipeline))
    
    if not all_results:
        print("No articles found for year 2015.")
        return
    
    # Get top 5 sources by count, but also include any ties at 5th position
    top_5 = all_results[:5]
    
    if len(top_5) >= 5:
        fifth_count = top_5[4]['count']
        # Track sources already included in top 4 (to avoid duplicates)
        included_sources = {item['_id'] for item in top_5[:4]}
        # Find all sources with the same count as the 5th item, excluding those already in top 4
        tied_sources = [
            item for item in all_results 
            if item['count'] == fifth_count and item['_id'] not in included_sources
        ]
        # Combine top 4 with all tied at 5th position (deduplicated)
        result = top_5[:4] + tied_sources
    else:
        result = top_5
    
    # Print results
    print("\nTop 5 news sources by article count (2015) (including ties at 5th position):")
    for i, result_item in enumerate(result, 1):
        source = result_item['_id']
        count = result_item['count']
        print(f"{i}. {source}: {count} articles")

# Handle option 4: 5 Most Recent Articles by Source
def handle_recent_by_source(collection):
    source_name = input("Enter source name: ").strip()
    
    if not source_name:
        print("Source name cannot be empty.")
        return
    
    # Work with lowercase input and treat it as a literal inside the pipeline so
    # that special characters (if any) are not treated as MongoDB operators
    source_literal = source_name.lower()
    
    # Use aggregation pipeline for case-insensitive source matching and sorting
    # by the actual published datetime so "most recent" is correctly handled
    pipeline = [
        # 1) Match source
        {
            '$match': {
                '$expr': {
                    '$eq': [
                        {'$toLower': '$source'},
                        {'$literal': source_literal}
                    ]
                }
            }
        },
        # 2) Convert published to date for proper sorting by datetime
        {
            '$project': {
                'title': 1,
                'published': 1,
                'publishedDate': {
                    '$cond': {
                        'if': {'$eq': [{'$type': '$published'}, 'date']},
                        'then': '$published',
                        'else': {'$toDate': '$published'}
                    }
                },
                'dateOnly': {
                    '$dateToString': {
                        'format': '%Y-%m-%d',
                        'date': {
                            '$cond': {
                                'if': {'$eq': [{'$type': '$published'}, 'date']},
                                'then': '$published',
                                'else': {'$toDate': '$published'}
                            }
                        }
                    }
                }
            }
        },
        # 3) Sort by published date descending (most recent first)
        {'$sort': {'publishedDate': -1}},
        # 4) Project final format
        {
            '$project': {
                '_id': 0,
                'title': 1,
                'publishedDate': 1,
                'date': '$dateOnly'
            }
        }
    ]
    
    # Get all articles for this source
    all_articles = list(collection.aggregate(pipeline))
    
    if not all_articles:
        print(f"Source '{source_name}' was not found in the database.")
        return
    
    # Get top 5, then include all articles tied at 5th position (same published datetime)
    top_5 = all_articles[:5]
    
    if len(top_5) >= 5:
        # Get the publishedDate of the 5th article for tie comparison
        fifth_published_date = top_5[4].get('publishedDate')
        if fifth_published_date:
            # Track articles already included (using title + publishedDate as unique key)
            included_keys = {(article.get('title', ''), article.get('publishedDate')) for article in top_5[:4]}
            
            # Find all articles with the same publishedDate as the 5th item
            # Exclude articles already in top 4 to avoid duplicates
            tied_articles = [
                article for article in all_articles 
                if (article.get('publishedDate') == fifth_published_date and 
                    (article.get('title', ''), article.get('publishedDate')) not in included_keys)
            ]
            # Combine top 4 with all tied at 5th position (deduplicated)
            result = top_5[:4] + tied_articles
        else:
            result = top_5
    else:
        result = top_5
    
    # Print Results
    print(f"\n5 Most Recent Articles from '{source_name}' are:")
    for i, article in enumerate(result, 1):
        title = article.get('title', 'N/A')
        date_str = article.get('date', 'Date not available')
        print(f"{i}. {title} ({date_str})")

# Main program
def main():
    # Validate command-line arguments
    if len(sys.argv) != 2:
        print("Usage is as follows: python3 phase2_query.py <port>", file=sys.stderr)
        sys.exit(1)
    
    port = sys.argv[1]
    # Try to convert the port string to an integer
    try:
        port = int(port)
    except ValueError:
        print(f"Error: Port must be a number, got '{sys.argv[1]}'", file=sys.stderr)
        sys.exit(1)

    # Connect to MongoDB server on localhost at the given port
    try:
        client = MongoClient('localhost', port)
        client.admin.command('ping')
    except Exception as e:
        # If connection fails, print error and exit
        print(f"Error: Cannot connect to MongoDB on port {port}: {e}", file=sys.stderr)
        sys.exit(1)
    
    db = client['291db']
    collection = db['articles']
    
    if 'articles' not in db.list_collection_names():
        # If the data hasn't been loaded yet, tell the user how to fix it
        print("Error: 'articles' collection not found. Please run load-json.py first.", file=sys.stderr)
        client.close()
        sys.exit(1)
    
    # Main loop to show menu and handle user choices
    while True:
        print_menu()
        choice = input("\nEnter a choice between 1 to 5: ").strip()
        
        if choice == '1':
            handle_common_words(collection)
        elif choice == '2':
            handle_article_count(collection)
        elif choice == '3':
            handle_top_sources_2015(collection)
        elif choice == '4':
            handle_recent_by_source(collection)
        elif choice == '5':
            print("The program will now exit, Team npm_run_dev wishes you well, have a good day!")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")
    
    client.close()

if __name__ == "__main__":
    main()
