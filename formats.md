**Формат хранения данных о видео (json)**:
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

**Формат хранения данных о ключах следующих страниц (json)**:
```
{
	"query_1": nextPageToken_1,
	"query_2": nextPageToken_2,
	...
}
```