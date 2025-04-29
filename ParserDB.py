import googleapiclient.discovery
from googleapiclient.errors import HttpError
import json
import numpy as np
import pandas as pd
import sqlite3
import csv

class ParserDB:
    """
    Provides convenient loading of videos and their comments based on search queries.
    Uses the YouTube Data API for parsing and SQLite3 for data storage.
    """
    def __init__(self, api_key_file: str, database: str = "data.db"):
        with open(api_key_file, "r", encoding="utf8") as f:
            self.api_key = f.read()
        self.connection = sqlite3.connect(database)
        self.cursor = self.connection.cursor()
        self.__init_db()

        self.youtube = googleapiclient.discovery.build(
            "youtube",
            "v3",
            developerKey=self.api_key
        )

    def __init_db(self):
        """
        Creates tables to store data about videos and comments,
        as well as a table for storing pagination offsets.
        """
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Videos (
                videoId TEXT PRIMARY KEY,
                query TEXT NOT NULL,
                commentsParsed INTEGER NOT NULL DEFAULT 0,
                channelId TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                publishedAt TEXT NOT NULL,
                viewCount INTEGER,
                likeCount INTEGER,
                commentCount INTEGER
            );
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Comments (
                commentId TEXT PRIMARY KEY,
                topLevelComment TEXT NOT NULL,
                videoId TEXT NOT NULL,
                authorDisplayName TEXT NOT NULL,
                text TEXT NOT NULL,
                publishedAt TEXT NOT NULL,
                likeCount INTEGER NOT NULL
            );
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Offsets (
                query TEXT PRIMARY KEY,
                next_page_token TEXT NOT NULL
            );
        """)
        self.connection.commit()

    def __del__(self):
        self.connection.close()

    def __extract_next_page_token(self, query) -> str | None:
        """
        Returns the page token from which to start searching for videos for a given query.
        If the query hasn't been searched before, returns None.
        """
        self.cursor.execute("SELECT next_page_token FROM Offsets WHERE query = ?", ((query,)))
        result = self.cursor.fetchone()
        return result if result is None else result[0]

    def __update_next_page_token(self, query, next_page_token) -> None:
        """
        Updates the next_pages_file with information about the page
        from which to start parsing videos for the given query.
        """
        self.cursor.execute("INSERT OR IGNORE INTO Offsets (query, next_page_token) VALUES (?, '0')", (query,))
        self.cursor.execute("UPDATE Offsets SET next_page_token = ? WHERE query = ?", (next_page_token, query))
        self.connection.commit()

    def __save_videos(self, response_search: dict, response_videos: dict, query: str) -> None:
        """
        Processes responses from Search and Videos requests.
        Saves video information into the Videos table of the database.
        """
        response_items = response_search["items"]
        new_videos = []
        
        for i, v in enumerate(response_items):
            statistics = response_videos["items"][i]["statistics"]
            description = response_videos["items"][i]["snippet"]["description"]
            
            new_videos.append(
                {
                    "query": query,
                    "videoId": v["id"]["videoId"],
                    "channelId": v["snippet"]["channelId"],
                    "publishedAt": v["snippet"]["publishedAt"],
                    "title": v["snippet"]["title"],
                    "description": description,
                    "viewCount": statistics.get("viewCount"),
                    "likeCount": statistics.get("likeCount"),
                    "commentCount": statistics.get("commentCount")
                }
            )

        self.cursor.executemany("""
            INSERT OR IGNORE INTO Videos (
                videoId, query, channelId, title, 
                description, publishedAt, viewCount, 
                likeCount, commentCount
            )
            VALUES (
                :videoId, :query, :channelId, :title, 
                :description, :publishedAt, :viewCount, 
                :likeCount, :commentCount
            )
        """, new_videos)
        self.connection.commit()

    def __save_comments(self, threads: list, video_id: str) -> int:
        """
        Takes a list of comment threads (in the format returned by
        the __parse_video_comments method) and saves comments into the
        Comments table of the database. Also sets the commentsParsed flag to 1
        in the Videos table for the video whose comments were saved.
        Returns the number of saved comments.
        """
        new_comments = []
        
        for thread in threads:
            raw_comments = [thread["snippet"]["topLevelComment"]] + \
                           ([] if "replies" not in thread else thread["replies"]["comments"])
            
            for raw_comment in raw_comments:
                new_comments.append(
                    {
                        "commentId": raw_comment["id"],
                        "topLevelComment": thread["snippet"]["topLevelComment"]["id"],
                        "videoId": video_id,
                        "authorDisplayName": raw_comment["snippet"]["authorDisplayName"],
                        "text": raw_comment["snippet"]["textDisplay"],
                        "publishedAt": raw_comment["snippet"]["publishedAt"],
                        "likeCount": raw_comment["snippet"]["likeCount"]
                    }
                )

        self.cursor.executemany("""
            INSERT INTO Comments (
                commentId, topLevelComment, videoId,
                authorDisplayName, text,
                publishedAt, likeCount
            )
            VALUES (
                :commentId, :topLevelComment, :videoId,
                :authorDisplayName, :text,
                :publishedAt, :likeCount
            )
        """, new_comments)
        self.cursor.execute("UPDATE Videos SET commentsParsed = 1 WHERE videoId = ?", (video_id,))
        self.connection.commit()

        return len(new_comments)

    def __search_request(
        self,
        query: str,
        next_page_token: str = None,
        max_request_results: int = 50,
    ) -> dict:
        """
        Performs a video search using the provided parameters.
        Returns the raw API response.
        """
        request = self.youtube.search().list(
            part="snippet",
            maxResults=max_request_results,
            q=query,
            pageToken=next_page_token,
            type="video",
            relevanceLanguage="ru",
            order="relevance" # viewCount?
        )
        response = request.execute()
        return response

    def __videos_request(self, response_search: list) -> dict:
        """
        Retrieves additional characteristics of videos obtained using
        __search_request(). Accepts the raw API response from the Search request.
        """
        videos = response_search["items"]
        video_ids = ",".join([video["id"]["videoId"] for video in videos])
        request = self.youtube.videos().list(
            part="snippet,statistics",
            id=video_ids
        )
        response = request.execute()
        return response

    def __commentThreads_request(self, video_id: str, next_page_token: str) -> dict:
        """
        Retrieves comments for a video by its ID.
        Returns the raw response from CommentThreads.list
        """
        request = self.youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            order="relevance",
            textFormat="plainText",
            pageToken=next_page_token,
            maxResults=100
        )
        try:
            response = request.execute()
        except HttpError as err:
            if "disabled comments" in err._get_reason():
                response = dict(items=[])
            else:
                print(video_id)
                response = dict(items=[])
                # raise err
            
        return response

    def __comments_list_request(self, parent_id: str, next_page_token: str):
        """
        Retrieves reply comments for a parent comment by its ID.
        """
        request = self.youtube.comments().list(
            parentId=parent_id,
            pageToken=next_page_token,
            maxResults=100,
            textFormat="plainText",
            part="snippet"
        )
        response = request.execute()
        return response

    def __parse_video_comments(self, video_id) -> list:
        """
        Loads all pages of comments for a video and returns
        the data in the form of threads.
        """
        next_page_token = None
        responses = []
        while True:
            response = self.__commentThreads_request(video_id, next_page_token)
            # There is a topComment and there are replies. If the number of replies is less than totalReplyCount,
            # a separate Comments.list request must be made
            for thread in response["items"]:
                if "replies" in thread and thread["snippet"]["totalReplyCount"] > len(thread["replies"]):
                    thread["replies"]["comments"] = []
                    next_replies_token = None
                    while True:
                        parent_id = thread["snippet"]["topLevelComment"]["id"]
                        replies_response = self.__comments_list_request(parent_id, next_replies_token)
    
                        thread["replies"]["comments"] += replies_response["items"]
                        
                        if "nextPageToken" not in replies_response:
                            break
                        next_replies_token = replies_response["nextPageToken"]
            
            responses.append(response)
            
            if "nextPageToken" not in response:
                break
            next_page_token = response["nextPageToken"]
            
        return [thread for response in responses for thread in response["items"]]

    def __graph_from_threads(self, threads_by_videos):
        nodes = set()
        edges = set()
        
        reply_count = 0
        problem_count = 0
        for videoId, videoThreads, query in threads_by_videos:
            videoThreads = json.loads(videoThreads)
            users = set()
            thread_authors = dict() # {topLevelComment: author}
            nodes.add((videoId, "video", query))
            for thread in videoThreads:
                thread = json.loads(thread)
                for comment in thread:
                    users.add(comment["authorDisplayName"])
                    nodes.add((comment["authorDisplayName"], "user", "USER"))
                    if comment["topLevelComment"] == comment["commentId"]:
                        thread_authors[comment["topLevelComment"]] = comment["authorDisplayName"]
                    
            for thread in videoThreads:
                thread = json.loads(thread)
                thread_author = thread_authors[thread[0]["topLevelComment"]]
                for comment in thread:
                    if comment["authorDisplayName"] == thread_author:
                        edges.add((comment["authorDisplayName"], videoId))
                    elif "@" not in comment["text"]:
                        edges.add((comment["authorDisplayName"], thread_author))
                    else:
                        reply_to = None
                        for user in users:
                            if user in comment["text"]:
                                reply_to = user
                                break
                        if reply_to is not None:
                            edges.add((comment["authorDisplayName"], reply_to))
                        else:
                            problem_count += 1
                            
        return nodes, edges, problem_count

    def parse_videos(
        self, 
        query: str,
        max_results: int = 50,
    ) -> None:
        """
        Loads videos using Search:list.
        query - the search term used to find videos | 
        max_results - maximum number of videos to load.
        """
        query = query.lower()

        videos_loaded = 0
        while videos_loaded < max_results:
            next_page_token = self.__extract_next_page_token(query)
                
            max_request_results = min(max_results - videos_loaded, 50)
            response_search = self.__search_request(query, next_page_token, max_results)
            if len(response_search["items"]) == 0:
                return
            response_videos = self.__videos_request(response_search)
            
            videos_loaded += len(response_search["items"])
            self.__save_videos(response_search, response_videos, query)

            if "nextPageToken" not in response_search:
                break
            next_page_token = response_search["nextPageToken"]
                
            self.__update_next_page_token(query, next_page_token)

            if len(response_search["items"]) < max_request_results:
                break

    def parse_comments(
        self,
        query,
        max_videos=5,
        verbose=False
    ) -> None:
        """
        Parses comments under videos found by the given query.
        Saves the data into the Comments table of the database.
        """
        self.cursor.execute("SELECT videoId FROM Videos WHERE query = ? AND NOT commentsParsed LIMIT ?",
                            ((query, max_videos)))
        videos = self.cursor.fetchall()
        if len(videos) == 0:
            print(f"Out of videos for '{query}' query.")
            return

        for video_id, in videos:
            threads = self.__parse_video_comments(video_id)
            comments_saved = self.__save_comments(threads, video_id)

            if verbose:
                print(f"Query \'{query}\', videoId {video_id}, {comments_saved} comments")

    def make_graph(self, folder: str):
        """
        Builds a graph showing interactions between commenters under videos.
        The graph is saved to files in the specified folder in the format
        required for import into Gephi.
        """
        self.cursor.execute("""
        SELECT videoId, videoThreads, query
        FROM
            (SELECT
                videoId,
                json_group_array(threads) AS videoThreads
            FROM
                (SELECT
                    topLevelComment,
                    videoId,
                    json_group_array(
                        json_object(
                            'commentId', commentId,
                            'topLevelComment', topLevelComment,
                            'text', text,
                            'authorDisplayName', authorDisplayName
                        )
                ) AS threads
                FROM
                    Comments
                GROUP BY
                    topLevelComment,
                    videoId
                )
            GROUP BY
                videoId) AS t1
            JOIN Videos USING(videoId)
        """)
        threads_by_videos = self.cursor.fetchall()
        nodes, edges, problem_count = self.__graph_from_threads(threads_by_videos)

        nodes = [dict(id=n[0], label=n[0], nodeType=n[1], query=n[2]) for n in nodes]
        edges = [dict(source=e[0], target=e[1]) for e in edges]
        nodes_df = pd.DataFrame(nodes)
        nodes_df["size"] = np.where(nodes_df["nodeType"] == "video", 20, 1)
        nodes_df.to_csv(f"{folder}/nodes.csv", index=False, encoding="utf-8")
        pd.DataFrame(edges).to_csv(f"{folder}/edges.csv", index=False, encoding="utf-8")
        return nodes, edges

    def describe(self):
        """
        Returns information about the number of videos and comments
        obtained for each query.
        """
        self.cursor.execute("SELECT query, COUNT(*) FROM Comments AS c JOIN Videos USING(videoId) GROUP BY query")
        comments_info = self.cursor.fetchall()
        return comments_info

    def comments_to_csv(self, query, output_file):
        """
        Produces a CSV table with parsed comments related to the given query.
        """
        self.cursor.execute("SELECT c.* FROM Comments AS c JOIN Videos AS v USING(videoId) WHERE query = ?", (query,))
        comments = self.cursor.fetchall()
        
        with open(f"{output_file}", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([header[0] for header in self.cursor.description])
            writer.writerows(comments)