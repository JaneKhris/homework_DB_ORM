import psycopg2
import sqlalchemy
import sqlalchemy as sq
import json
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), nullable=False)
    publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher1 = relationship(Publisher, backref="books")


class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=True)

    book1 = relationship(Book, backref="stocks")
    shop1 = relationship(Shop, backref="stocks")

class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Numeric, nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=True)

    stock1 = relationship(Stock, backref="sales")

def delete_tables(engine):
    Base.metadata.drop_all(engine)


def create_tables(engine):
    Base.metadata.create_all(engine)


def db_filling(ses):

    with open('tests_data.json', 'r') as fd:
        data = json.load(fd)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]

        ses.add(model(id=record.get('pk'), **record.get('fields')))
    ses.commit()


def find_publiher_id(ses):
    global publisher_id
    publisher_data = input('Введите наименование издателя или его порядковый номер в базе данных: ')
    if publisher_data.isnumeric():
        publisher_id = publisher_data
    else:
        q = ses.query(Publisher).filter(Publisher.name == publisher_data)
        for s in q.all():
            publisher_id = s.id
    return publisher_id


def find_shop(ses):
    pub_id = find_publiher_id(ses)
    q = ses.query(Book, Publisher, Stock, Shop)
    q = q.join(Publisher, Book.publisher == Publisher.id)
    q = q.join(Stock, Stock.book == Book.id)
    q = q.join(Shop, Shop.id == Stock.shop)
    q = q.filter(Publisher.id == pub_id)
    r = q.all()
    shop_list = []
    for book, publisher, stock, shop in r:
        shop_list.append(shop.name)
    shops_final =list(set(shop_list))
    print(shops_final)


def get_DSN(filename = 'data_connect.json'):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
        DSN_str = "postgresql://" + data.get('login') + ":" + data.get('password') + "@localhost:" + data.get(
            'localhost') + "/" + data.get('database')
    return DSN_str


if __name__ == "__main__":
    DSN = get_DSN()
    engine = sqlalchemy.create_engine(DSN)
    # delete_tables(engine)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()


    db_filling(session)
    find_shop(session)
