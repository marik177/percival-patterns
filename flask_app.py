from flask import Flask, request
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import orm
import config
import repository
import model


orm.start_mappers()
engine = create_engine(config.get_postgres_uri())
get_session = sessionmaker(bind=engine)
app = Flask(__name__)


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    session = get_session()
    batches = repository.SqlAlchemyRepository(session).list()
    line = model.OrderLine(
        request.json["orderid"], request.json["sku"], request.json["qty"],
    )

    batchref = model.allocate(line, batches)

    return {"batchref": batchref}, 201


if __name__ == '__main__':
    app.run()
