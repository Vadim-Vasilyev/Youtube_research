**Формат хранения данных о видео**:
```
{
	"query_1": [
			{
				"videoId": string,
				"publishedAt": datetime
				"channelId": string,
				"title": string,
				"description": string,
				"statistics": {
					"viewCount": int,
					"likeCount": int,
					"dislikeCount": int,
					"commentCount": int
				}
			},
			...
	],
	"query_2": [
	...
	]
}

```

**Формат хранения комментариев**:
```
{
    "videoId_1" : [
        {
            "topLevelComment": {
                "commentId": string,
                "authorDisplayName": string,
                "text": string,
                "likeCount": int,
                "publishedAt": datetime
            },
            "replies": [
                {
                    "commentId": string,
                    "authorDisplayName": string,
                    "text": string,
                    "parentId": string,
                    "likeCount": int,
                    "publishedAt": datetime
                },
                ...
            ]
        },
        ...
    ],
    ...
}
```

**Формат хранения информации, необходимой для корректной работы методов parse_videos и parse_comments между сессиями**:
```
{
    "Search.list": {
    	"query_1": nextPageToken_1,
    	"query_2": nextPageToken_2,
    	...
    },
    "CommentThreads.list": {
        "query_1": next_video_index
        ...
    }
}
```