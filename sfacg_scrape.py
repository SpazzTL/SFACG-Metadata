import requests
from bs4 import BeautifulSoup
import json
import time
import threading
from queue import Queue
import os

# Global variables for thread safety and shared state
lock = threading.Lock()
total_novels_found = 0
output_filename = "sfacg_novels.jsonl"
page_queue = Queue()
scraped_novel_ids = set()

# Scrape logic for a single page
def scrape_page(page_index, publication_status_code):
    """
    Scrapes a single page for a specific publication status and writes novel data to the file.
    Includes rate-limiting detection and handling.
    """
    global total_novels_found
    base_url = f"https://book.sfacg.com/List/default.aspx?if={publication_status_code}&&PageIndex="
    url = f"{base_url}{page_index}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    current_status = "连载中" if publication_status_code == 0 else "已完结"

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        novel_lists = soup.find_all('ul', class_='Comic_Pic_List')

        if not novel_lists:
            print(f"[{threading.current_thread().name}] Page {page_index} for '{current_status}': No novels found. This part of the scrape is complete.")
            return False 

        novels_on_page = 0
        with lock:
            with open(output_filename, 'a', encoding='utf-8') as f:
                for novel_list in novel_lists:
                    try:
                        novel_info_li = novel_list.find_all('li')[1]
                        
                        # Title and ID
                        title_tag = novel_info_li.find('strong').find('a')
                        novel_id = title_tag['href'].split('/')[2]

                        if novel_id in scraped_novel_ids:
                            continue

                        title = title_tag.text.strip()
                        
                        # Author
                        author_tag = novel_info_li.find('a', id=lambda x: x and 'AuthorLink' in x)
                        author = author_tag.text.strip() if author_tag else 'Unknown'

                        # Synopsis
                        # Use a more reliable method to get the last text part
                        contents = [c for c in novel_info_li.contents if c.name is None and c.strip()]
                        synopsis = contents[-1].strip() if contents else ""
                        
                        # Tags (Genre)
                        genre_tag = novel_info_li.find('a', href=lambda href: href and '/List/?tid=' in href)
                        tags = [genre_tag.text.strip()] if genre_tag else []

                        # Cover URL
                        cover_tag = novel_list.find('li', class_='Conjunction').find('img')
                        cover_url = cover_tag['src'] if cover_tag else ''

                        novel_data = {
                            "id": novel_id,
                            "title": title,
                            "synopsis": synopsis,
                            "author": author,
                            "tags": tags,
                            "is_adult": False,
                            "publication_status": current_status,
                            "cover_url": cover_url,
                            "source": "sfacg",
                            "time_scraped": time.strftime("%Y-%m-%dT%H:%M:%S")
                        }
                        
                        f.write(json.dumps(novel_data, ensure_ascii=False) + '\n')
                        scraped_novel_ids.add(novel_id)
                        novels_on_page += 1

                    except (AttributeError, IndexError) as e:
                        print(f"[{threading.current_thread().name}] Warning: Could not parse a novel on page {page_index} for '{current_status}'. Skipping. Error: {e}")
                        continue
        
        with lock:
            total_novels_found += novels_on_page
            print(f"[{threading.current_thread().name}] Page {page_index} ('{current_status}') scraped. Found {novels_on_page} NEW novels. Total: {total_novels_found}")
        
        return True

    except requests.exceptions.HTTPError as e:
        if e.response.status_code in [429, 503, 504]:
            print(f"[{threading.current_thread().name}] Rate-limited or blocked on page {page_index} ('{current_status}') (Status: {e.response.status_code}). Pausing for 60 seconds.")
            time.sleep(60)
            return scrape_page(page_index, publication_status_code)
        else:
            print(f"[{threading.current_thread().name}] HTTP Error on page {page_index} ('{current_status}'): {e}")
            return True

    except requests.exceptions.RequestException as e:
        print(f"[{threading.current_thread().name}] Request Exception on page {page_index} ('{current_status}'): {e}. Retrying in 10 seconds.")
        time.sleep(10)
        return scrape_page(page_index, publication_status_code)


def worker(publication_status_code):
    """Thread worker function to process pages from the queue."""
    while True:
        page_index = page_queue.get()
        if page_index is None:
            break
        
        scrape_page(page_index, publication_status_code)
        page_queue.task_done()

def main():
    """Main function to setup threads and manage the scraping process."""
    global total_novels_found
    global output_filename
    global scraped_novel_ids

    if os.path.exists(output_filename):
        print(f"Found existing file '{output_filename}'. Loading existing novel IDs to avoid duplicates...")
        with open(output_filename, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    scraped_novel_ids.add(data.get('id'))
                except json.JSONDecodeError:
                    print("Warning: Skipping malformed line in existing file.")
        total_novels_found = len(scraped_novel_ids)
        print(f"Found {total_novels_found} novels in the existing file. Will append new novels.")
    else:
        print(f"No existing file found. A new file '{output_filename}' will be created.")

    while True:
        try:
            thread_count = int(input("Enter the number of threads to use (e.g., 5-10): "))
            if thread_count > 0:
                break
            else:
                print("Please enter a positive integer.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    for publication_status_code in [0, 1]: # Scrape ongoing (0) and completed (1) novels separately
        current_status = "连载中" if publication_status_code == 0 else "已完结"
        print(f"\nStarting to scrape all '{current_status}' novels...")
        
        # Create and start worker threads for this status
        workers = []
        for i in range(thread_count):
            t = threading.Thread(target=worker, args=(publication_status_code,), name=f"Thread-{i+1}-{current_status[:2]}")
            t.daemon = True
            t.start()
            workers.append(t)
        
        # Populate the queue with page numbers
        page_index = 1
        while True:
            # Check if we should stop. A more robust way to do this with threads
            # would be to check a queue for a "stop" signal, but the simple
            # page check in scrape_page is sufficient for this scenario.
            if page_index > 2000: # Safety limit
                print(f"Safety limit of 2000 pages reached for '{current_status}'. Stopping this phase.")
                break
            page_queue.put(page_index)
            page_index += 1

        # Wait for all tasks to be completed for the current status
        page_queue.join()

        # Stop the worker threads for the current status
        for _ in range(thread_count):
            page_queue.put(None)
        for t in workers:
            t.join()

    print(f"\nScraping finished. All data saved to '{output_filename}'.")
    print(f"Final total novels scraped: {total_novels_found}")

if __name__ == "__main__":
    main()