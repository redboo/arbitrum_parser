import click

from scraper import Scraper


@click.command()
@click.option(
    "--lastdays",
    default=None,
    type=int,
    help="Установите интервал в секундах для автоматического парсинга (по умолчанию не установлен)",
)
@click.option(
    "--csv",
    is_flag=True,
    default=False,
    help="Укажите этот параметр, чтобы сохранить данные в CSV-файл (по умолчанию: сохранять)",
)
@click.option(
    "--excel",
    is_flag=True,
    default=False,
    help="Укажите этот параметр, чтобы сохранить данные в Excel-файл (по умолчанию: не сохранять)",
)
def run(
    lastdays: int = 0,
    csv: bool = False,
    excel: bool = False,
) -> None:
    parser = Scraper()
    parser.process_categories()
    file_format = "excel" if excel else "csv"
    parser.process_topics_and_save(file_format=file_format, last_date=lastdays)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем.")
