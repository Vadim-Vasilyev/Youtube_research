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

    def __extract_next_page_token(self, query):
        """
        Возвращает токен страницы, с которой нужно начинать поиск видео по данному запросу.
        Если по данному запросу ещё не производился поиск, возвращает None
        """
        
        next_page_token = None
        try:
            with open(self.next_pages_file, "r") as next_pages_file:
                next_pages = json.loads(next_pages_file.read())
                if query in next_pages:
                    next_page_token = next_pages[query]
        # create file if it does not exist
        except FileNotFoundError:
            with open(self.next_pages_file, "w") as f:
                f.write(json.dumps({}))
        
        return next_page_token

    def __update_next_page_token(self, query, next_page_token):
        """
        Обновляет в next_pages_file информацию о странцие,
        с которой нужно начинать парсить видео по запросу query
        """
        with open(self.next_pages_file, "r") as f:
            tokens = json.loads(f.read())
        tokens[query] = next_page_token
        with open(self.next_pages_file, "w") as f:
            f.write(json.dumps(tokens, indent=2))

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
            self.__update_next_page_token(query, response_search["nextPageToken"])

            if len(response_search["items"]) < max_request_results:
                break

    def get_parsed_videos(self, query: str = None, video_file: str = None) -> dict | list:
        """
        Если query = None, возвращает словарь, где ключи - запросы,
        а значения - массивы с видео, полученными за всё время по этому запросу.
        Если query передан, возвращает один массив с видео, соответствующих
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