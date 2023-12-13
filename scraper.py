import csv
import logging
import os
from datetime import datetime, timedelta
from os import path
from time import sleep

import pandas as pd
import requests
from bs4 import BeautifulSoup
from icecream import ic

from config import DOMAIN, DOWNLOADS_DIR

ic.disable()


class Scraper:
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299"
    )

    def __init__(
        self,
        last_days: int = 0,
    ) -> None:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Парсинг запущен...")
        if last_days != 0:
            self.start = datetime.combine(
                datetime.now() - timedelta(days=last_days), datetime.min.time()
            )
        self.topics = []
        self.categories = []
        self.badges = {}
        self.status = {}
        self.domain = DOMAIN
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.USER_AGENT})

    def create_csv_file(self, filename_suffix, headers) -> str:
        filename: str = (
            f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{filename_suffix}.csv"
        )
        if not path.exists(filename):
            with open(filename, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(headers)
        return filename

    def fetch_topics(self):
        ic()
        url: str = f"{self.domain}latest.json"
        params = {"no_definitions": "true", "page": 0}

        while True:
            try:
                response = self.session.get(url, params=params, timeout=20)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 429:
                    raise

                print("Слишком много запросов, ожидание 5 секунд")
                sleep(10)
                continue
            data = response.json()
            if not data["topic_list"]["topics"]:
                break
            self.topics.extend(data["topic_list"]["topics"])
            params["page"] += 1

            if params["page"] % 10 == 0:
                print(f"Обработано {len(self.topics)} тем из {params['page']} страниц")

        print(f"Обработано {len(self.topics)} тем из {params['page']} страниц")

    def fetch_comments_in_topic(
        self,
        topic,
        last_date: int = 0,
    ):
        ic()
        # ic(last_date)
        comments_list = []
        url: str = f"{self.domain}t/{topic['slug']}/{topic['id']}"
        print(f"Собираю комментарии со страницы {url}")

        start_datetime = None
        if last_date != 0:
            start_datetime = (datetime.now() - timedelta(days=float(last_date))).date()
            ic(start_datetime)

        if "last_posted_at" in topic:
            topic_last_date = datetime.strptime(
                topic["last_posted_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
            ).date()
        else:
            topic_last_date = datetime.strptime(
                topic["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
            ).date()

        ic(topic_last_date)

        if start_datetime and topic_last_date <= start_datetime:
            ic(
                f"Топик {topic['title']} ({topic['id']}) старше указанной даты {topic_last_date}"
            )
            return comments_list

        while True:
            try:
                res = self.session.get(url, timeout=10)
                res.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"{e.response.status_code}. Страница {url} не cуществует.")
                    return comments_list
                if e.response.status_code == 403:
                    print(f"{e.response.status_code}. Страница {url} недоступна.")
                    return comments_list
                if e.response.status_code == 429:
                    print(
                        f"{e.response.status_code}. Слишком много запросов. Подождите 5 секунд."
                    )
                    sleep(10)
                    continue
                else:
                    logging.error("Ошибка при запросе: %s", e, exc_info=True)
                    raise e
            soup = BeautifulSoup(res.text, "html.parser")
            comments = soup.find_all("div", {"class": "crawler-post"})

            topic_category_elements = soup.select("div.topic-category a")
            topic_categories = [
                topic_category.get_text().strip()
                for topic_category in topic_category_elements
            ]
            proposal = {"categories": ", ".join(topic_categories) or ""}
            next_page_link = None

            for i, comment in enumerate(comments):
                # ic(comment)
                if next_page_link := comment.find("a", rel="next"):
                    next_page_link = next_page_link.get("href").split("?")[1]
                    break
                if comment.find("a", rel="prev"):
                    return comments_list

                post_id = int(comment["id"].split("_")[-1])
                comment_author = comment.find("span", itemprop="name").text.strip()

                date = comment.find("time").get("datetime")
                date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
                comment_date: str = date.strftime("%Y-%m-%d")

                comment_likes = comment.find("meta", itemprop="userInteractionCount")[
                    "content"
                ]
                comment_text = (
                    comment.find("div", {"class": "post"})
                    .text.strip()
                    .replace("\n", " ")
                )
                comment_url = f"{url}/{post_id}"

                if i == 0:
                    proposal["text"] = comment_text
                    proposal["author"] = comment_author
                    proposal["likes"] = comment_likes
                    continue

                if start_datetime and date.date() <= start_datetime:
                    continue

                comment_author_badges = self.fetch_user_badges(comment_author)
                comment_author_status = self.fetch_user_status(comment_author)

                comments_list.append(
                    [
                        topic["title"],
                        datetime.strptime(
                            topic["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
                        ).strftime("%Y-%m-%d"),
                        comment_url,
                        proposal["author"],
                        proposal["categories"],
                        proposal["text"],
                        proposal["likes"],
                        comment_text,
                        comment_likes,
                        comment_date,
                        comment_author,
                        comment_author_status,
                        comment_author_badges,
                    ]
                )

            if next_page_link:
                url = f"{url.split('?')[0]}?{next_page_link}"
            else:
                return comments_list

    def fetch_categories(self):
        ic()
        url = f"{self.domain}categories.json"

        while True:
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 429:
                    raise

                print("Too many requests, waiting 10 seconds")
                sleep(10)
                continue

            data = response.json()
            if not data["category_list"]["categories"]:
                return []

            print("Categories have been retrieved")
            return data["category_list"]["categories"]

    def process_categories(self) -> None:
        ic()
        categories = self.fetch_categories()
        # ic(categories)
        self.categories = {category["id"]: category["name"] for category in categories}

    def fetch_user_badges(self, username):
        if username in self.badges:
            return self.badges[username]

        url = f"{self.domain}user-badges/{username}.json?grouped=true"

        while True:
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.badges[username] = ""
                    return self.badges[username]
                if e.response.status_code != 429:
                    raise

                print("Too many requests, waiting 10 seconds")
                sleep(10)
                continue

            data = response.json()
            if "badges" not in data:
                return

            self.badges[username] = ", ".join(
                [badge["name"] for badge in data["badges"]]
            )
            return self.badges[username]

    def fetch_user_status(self, username):
        if username in self.status:
            return self.status[username]

        url: str = f"{self.domain}u/{username}.json"

        while True:
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.status[username] = ""
                    return self.status[username]
                if e.response.status_code != 429:
                    raise

                print("Too many requests, waiting 10 seconds")
                sleep(10)
                continue

            data = response.json()
            if not data["user"] and not data["user"]["title"]:
                return

            self.status[username] = data["user"]["title"]
            return self.status[username]

    def process_topics(self, limit: int = 0) -> None:
        ic()
        self.fetch_topics()
        topics_to_process = self.topics if limit == 0 else self.topics[:limit]
        for topic in topics_to_process:
            comments = self.fetch_comments_in_topic(topic, last_date=7)
            ic(len(comments))
            ic(comments)

    def comments_to_dataframe(self, comments_list) -> pd.DataFrame:
        """Converts the comments list to a pandas DataFrame."""
        df_columns: list[str] = [
            "Proposal/Post name",
            "Proposal Date",
            "Comment Link",
            "Proposal Author",
            "Proposal Category",
            "Proposal Text",
            "Proposal Likes",
            "Comment Text",
            "Comment Likes",
            "Comment Date",
            "Comment Author",
            "User's status",
            "User's Badges",
        ]
        return pd.DataFrame(comments_list, columns=df_columns)

    def save_comments(self, comments_list, filename_suffix, file_format="csv") -> None:
        """
        Saves the comments to a file in the specified format.

        Args:
            comments_list: List of comments to be saved.
            filename_suffix: Suffix to be added to the filename.
            file_format: The format of the file to save the data ['csv', 'xlsx'].
        """
        if file_format == "excel":
            file_format = "xlsx"
        filename = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{filename_suffix}.{file_format}"

        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        filename: str = os.path.join(DOWNLOADS_DIR, filename)
        ic(filename)

        df: pd.DataFrame = self.comments_to_dataframe(comments_list)

        if file_format == "csv":
            df.to_csv(filename, index=False, encoding="utf-8-sig")
        elif file_format == "xlsx":
            df.to_excel(filename, index=False)

        print(f'Комментарии сохранены в "{filename}"')

    def process_topics_and_save(
        self, limit: int = 0, file_format="csv", last_date: int = 0
    ) -> None:
        self.fetch_topics()
        topics_to_process = self.topics if limit == 0 else self.topics[:limit]
        comments = []
        for topic in topics_to_process:
            comments.extend(self.fetch_comments_in_topic(topic, last_date=last_date))

        self.save_comments(
            comments,
            filename_suffix="forum.arbitrum.foundation",
            file_format=file_format,
        )
        ic(len(comments))


if __name__ == "__main__":
    parser = Scraper()
    parser.process_categories()
    parser.process_topics_and_save(file_format="csv", last_date=7)
