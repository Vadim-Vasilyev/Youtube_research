import googleapiclient.discovery
import json

class Parser:
    """
    Позволяет удобно загружать видео и комментарии к ним по запросу
    Использует методы Search.list, CommentThreads.list и Videos.list
    """
    
    def __init__(
        self,
        video_file: str,      # Путь к файлу, куда будет сохраняться информация о загруженных видео
        comment_file: str,    # Путь к файлу, куда будет сохраняться информация о загруженных комментариях
        next_pages_file: str, # Путь к файлу, хранящему токены страниц, с которых надо начинать поиск для каждого запроса
        api_key: str = None,         # Ключ Youtube API
        api_key_file: str = None,    # Путь к файлу, где записан ключ Youtube API
    ):
        if api_key is None and api_key_file is None:
            raise ValueError("Provide api_key or api_key_file")
        self.api_key = api_key if api_key is not None else self.__extract_key(api_key_file)

        self.video_file = video_file
        self.comment_file = comment_file
        self.next_pages_file = next_pages_file

        self.youtube = googleapiclient.discovery.build(
            "youtube",
            "v3",
            developerKey=self.api_key
        )
        
    def __extract_key(self, api_key_file: str) -> str:
        """
        Возвращает содержание api_key_file, которое будет использоваться как токен.
        """
        with open(api_key_file, "r") as f:
            api_key = f.read()
        return api_key

    def __extract_next_page_token(self, query) -> str | None:
        """
        Возвращает токен страницы, с которой нужно начинать поиск видео по данному запросу.
        Если по данному запросу ещё не производился поиск, возвращает None
        """
        
        next_page_token = None
        try:
            with open(self.next_pages_file, "r") as next_pages_file:
                next_pages = json.loads(next_pages_file.read())["Search.list"]
                if query in next_pages:
                    next_page_token = next_pages[query]
        # create file if it does not exist
        except FileNotFoundError:
            with open(self.next_pages_file, "w") as f:
                f.write(json.dumps({"Search.list": {}, "CommentThreads.list":  {}}))
        
        return next_page_token

    def __extract_next_video_idx(self, video_file, query) -> str | None:
        """
        Возвращает индекс видео, с которого необходимо продолжить
        парсинг комментариев из video_file для запроса query
        """
        next_video_idx = 0
        try:
            with open(self.next_pages_file, "r") as next_pages_file:
                next_videos = json.loads(next_pages_file.read())["CommentThreads.list"]
                if query in next_videos:
                    next_video_idx = next_videos[query]
        except FileNotFoundError:
            with open(self.next_pages_file, "w") as f:
                f.write(json.dumps({"Search.list": {}, "CommentThreads.list":  {}}))

        return next_video_idx

    def __update_next_page_token(self, query, next_page_token):
        """
        Обновляет в next_pages_file информацию о странцие,
        с которой нужно начинать парсить видео по запросу query
        """
        with open(self.next_pages_file, "r") as f:
            tokens = json.loads(f.read())
        tokens["Search.list"][query] = next_page_token
        with open(self.next_pages_file, "w") as f:
            f.write(json.dumps(tokens, indent=2))

    def __update_next_video_idx(self, video_file, query):
        """
        Обновляет в next_pages_file информацию о том, с какого
        видео нужно начинать парсить комментарии по запросу query
        """
        with open(self.next_pages_file, "r") as f:
            indices = json.loads(f.read())
        indices["CommentThreads.list"][query] = indices["CommentThreads.list"].get(query, 0) + 1
        with open(self.next_pages_file, "w") as f:
            f.write(json.dumps(indices, indent=2))

    def __save_videos(self, response_search: dict, response_videos: dict, query: str, file: str) -> None:
        """
        Обрабатывает json, полученный в результате Search-запроса.
        Сохраняет информацию о видео в video_file
        """
        response_items = response_search["items"]
        new_query_videos = []
        
        for i, v in enumerate(response_items):
            statistics = response_videos["items"][i]["statistics"]
            statistics.pop("favoriteCount")
            
            new_query_videos.append(
                {
                    "videoId": v["id"]["videoId"],
                    "publishedAt": v["snippet"]["publishedAt"],
                    "channelId": v["snippet"]["channelId"],
                    "title": v["snippet"]["title"],
                    "description": v["snippet"]["description"],
                    "statistics": statistics
                }
            )

        try:
            with open(file, "r") as f:
                all_videos = json.loads(f.read())
        except FileNotFoundError:
            all_videos = {}

        all_videos[query] = all_videos.get(query, []) + new_query_videos
        with open(file, "w") as f:
            f.write(json.dumps(all_videos, indent=2))

    def __save_comments(self, threads: list, video_id: str, comment_file: str = None) -> None:
        if comment_file is None:
            comment_file = self.comment_file

        try:
            with open(comment_file, "r") as f:
                all_threads = json.loads(f.read())
        except FileNotFoundError:
            with open(comment_file, "w") as f:
                f.write(json.dumps({}))
                all_threads = {}

        total_comments = 0
        items = []
        for thread in threads:
            item = {}
            top_level_comment = thread["snippet"]["topLevelComment"]
            item["topLevelComment"] = {
                "commentId": top_level_comment["id"],
                "authorDisplayName": top_level_comment["snippet"]["authorDisplayName"],
                "text": top_level_comment["snippet"]["textDisplay"],
                "likeCount": top_level_comment["snippet"]["likeCount"],
                "publishedAt": top_level_comment["snippet"]["publishedAt"]
            }
            if "replies" in thread:
                item["replies"] = [{
                    "commentId": reply["id"],
                    "authorDisplayName": reply["snippet"]["authorDisplayName"],
                    "text": reply["snippet"]["textDisplay"],
                    "likeCount": reply["snippet"]["likeCount"],
                    "publishedAt": reply["snippet"]["publishedAt"]
                } for reply in thread["replies"]["comments"]]
            else: item["replies"] = []

            items += [item]
            total_comments += 1 + len(item["replies"])

        all_threads[video_id] = all_threads.get(video_id, [])  + items
        with open(comment_file, "w") as f:
            f.write(json.dumps(all_threads, indent=2))

        return total_comments
        
    def __search_request(
        self,
        query: str,
        next_page_token: str = None,
        max_request_results: int = 50,
    ) -> dict:
        """
        Выполняет поиск видео по переданным параметрам. Возвращает
        сырой ответ API.
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
        Выполняет получение дополнительных характеристик видео, полученных с помощью
        __search_request(). Принимает сырой ответ API на Search-запрос.
        """
        videos = response_search["items"]
        video_ids = ",".join([video["id"]["videoId"] for video in videos])
        request = self.youtube.videos().list(
            part="statistics",
            id=video_ids
        )
        response = request.execute()
        return response

    def __commentThreads_request(self, video_id: str, next_page_token: str) -> dict:
        """
        Выполняет получение комментариев к видео по его id.
        Возвщает сырой ответ CommentThreads.list
        """
        request = self.youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            order="relevance",
            textFormat="plainText",
            pageToken=next_page_token,
            maxResults=100
        )
        response = request.execute()
        return response

    def __comments_list_request(self, parent_id: str, next_page_token: str):
        """
        Выполняет получение комментариев-ответов на родительский комментарий
        по его id
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

    def __parse_video_comments(self, video_id):
        """
        Загружает и возвращает все страницы комментариев к видео по его id
        """
        next_page_token = None
        responses = []
        while True:
            response = self.__commentThreads_request(video_id, next_page_token)
            # есть topComment, есть replies. Если реплаев меньше, чем totalReplyCount,
            # нужно делать отдельный запрос Comments.list
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

    def parse_videos(
        self, 
        query: str,
        max_results: int = 50,
        start_from_next_page: bool = True,
        video_file: str = None
    ) -> None:
        """
        Загружает ролики с помощью Search:list\n
        query - запрос, по которому будут найдены ролики | 
        max_results - максимальное количество видео для загрузки | 
        start_from_next_page - если True, попытаемся найти в next_pages_file токен страницы,
        с которой надо начинать поиск для этого запроса. Если False, будем начинать поиск с
        первой страницы | video_file - если не передан, результат сохраняется в собственный
        файл, полученный при инициализации. Иначе сохраняется в переданный файл
        """ 
        if video_file is None:
            video_file = self.video_file
            
        query = query.lower()

        next_page_token = None
        videos_loaded = 0
        while videos_loaded < max_results:
            if start_from_next_page:
                next_page_token = self.__extract_next_page_token(query)
                
            max_request_results = min(max_results - videos_loaded, 50)
            response_search = self.__search_request(query, next_page_token, max_results)
            if len(response_search["items"]) == 0:
                return
            response_videos = self.__videos_request(response_search)
            
            videos_loaded += len(response_search["items"])
            self.__save_videos(response_search, response_videos, query, video_file)

            if "nextPageToken" not in response_search:
                break
            next_page_token = response_search["nextPageToken"]
                
            self.__update_next_page_token(query, next_page_token)

            if len(response_search["items"]) < max_request_results:
                break

    def parse_comments(
        self,
        query,
        video_file=None,
        max_videos=5,
        comment_file=None,
        verbose=False
    ) -> None:
        """
        Парсит комментарии всех полученных по запросу query видео
        из video_file, сохраняет данные о них в comment_file.
        Поддерживает возможность обрабатывать один файл по частям.
        """
        if video_file is None:
            video_file = self.video_file
        if comment_file is None:
            comment_file = self.comment_file
            
        with open(video_file, "r") as f:
            videos = json.loads(f.read())
            
        video_processed = 0
        while video_processed < max_videos:
            idx = self.__extract_next_video_idx(video_file, query)
            if idx == len(videos[query]):
                break
            video_id = videos[query][idx]["videoId"]
            
            threads = self.__parse_video_comments(video_id)
            comments_saved = self.__save_comments(threads, video_id, comment_file)
            
            self.__update_next_video_idx(video_file, query)
            video_processed += 1

            if verbose:
                print(f"Query \'{query}\', video №{idx}, videoId {video_id}, {comments_saved} comments")

    def get_parsed_videos(self, query: str = None, video_file: str = None) -> dict | list:
        """
        Если query = None, возвращает словарь, где ключи - запросы,
        а значения - массивы с видео, полученными за всё время по этому запросу.
        Если query передан, возвращает один массив с видео, соответствующими
        такому запросу. |
        Если video_file не передан, результаты будут получены из собственного
        файла, заданного при инициализации. Иначе результат будет получен из
        переданного файла.
        """
        if video_file is None:
            video_file = self.video_file
            
        try:
            with open(video_file, "r") as f:
                videos = json.loads(f.read())
                if query:
                    return videos.get(query, [])
                return videos
        except FileNotFoundError:
            return [] if query else {}

    def get_parsed_comments(self, video_id: str = None, comment_file: str = None) -> dict | list:
        """
        Если video_id = None, возвращает словарь, где ключи - videoId,
        а значения - массивы со всеми тредами комментариев для данного видео.
        Если video_id передан, возвращает один массив с тредами, соответствующими
        данному видео.
        Если comment_file не передан, результаты будут получены из собственного
        файла, заданного при инициализации. Иначе результат будет получен из
        переданного файла.
        """
        if comment_file is None:
            comment_file = self.comment_file

        try:
            with open(comment_file, "r") as f:
                threads = json.loads(f.read())
                if video_id:
                    return threads.get(video_id, [])
                return threads
        except FileNotFoundError:
            return [] if video_id else {}
    
    def join_video_files(self, video_file_1: str, video_file_2: str, output: str) -> None:
        """
        Объединяет два файла, хранящие информацию о видео,
        в один новый файл - output. Старые файлы не удаляются
        """
        with open(video_file_1, "r") as fin_1, \
        open(video_file_2, "r") as fin_2, \
        open(output, "w") as f_out:
            videos_1 = json.loads(fin_1.read())
            videos_2 = json.loads(fin_2.read())
            videos_new = {}
            for query in set(videos_1.keys()).union(set(videos_2.keys())):
                videos_new[query] = videos_1.get(query, []) + videos_2.get(query, [])
            f_out.write(json.dumps(videos_new, indent=2))