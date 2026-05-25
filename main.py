
from src.generate_data import extract_all_data

from src.create_session import create_spark_session

spark = create_spark_session()


def main():
    print("Hello world!")

    extract_all_data(spark)


if __name__ == "__main__":
    main()
