use wikipedia;


select  top 10 PageID, PAgeRevisionID, LEN(ArticleText), LEN(COMPRESS(ArticleText))
from PageRevisionText
WHERE NamespaceID = 0
ORDER BY LEN(ArticleText) DESC


set statistics io on

-- count of articles with text and their length stats
SELECT Namespace.NamespaceID, NamespaceName,
		count(PageID) AS PageCount,
		MIN(LEN(ArticleText)) AS MinLength,
		AVG(LEN(ArticleText)) AS AverageLength,
		MAX(LEN(ArticleText)) AS MaxLength
FROM PageRevisionText
JOIN Namespace ON Namespace.NamespaceID = PageRevisionText.NamespaceID
GROUP BY Namespace.NamespaceID, Namespace.NamespaceName
ORDER BY NamespaceID




-- , LEN(COMPRESS(ArticleText))

SELECT PageID, PageRevisionID, ArticleLength, LEN(COMPRESS(ArticleText)) CompressedLength
FROM
(
SELECT TOP 10 PageID, PAgeRevisionID, LEN(ArticleText) AS ArticleLength, ArticleTExt
from PageRevisionText
WHERE NamespaceID = 0
ORDER BY LEN(ArticleText) DESC
)
AS X


SELECT COMPRESS(ArticleText) FROM PageRevisionText
WHERE NamespaceID = 0 AND PageID = 102400	AND PageRevisionID = 1232052018
