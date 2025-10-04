from src.core import (
    ApplicationRozetka, 
    load_settings, 

)
from src.core.settings import load_settings

settings = load_settings()


def start_application(app_class):
    app = app_class()
    app.start()


def main() -> None:
    app_classes = [
        ApplicationRozetka,
    ]

    for app_class in app_classes:
        start_application(app_class)

    

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Завершение роботы пользователем.")
    except Exception as ex:
        pass

