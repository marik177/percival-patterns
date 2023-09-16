from flask import Flask, request, jsonify
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import orm
import config
import repository
import model
import services


orm.start_mappers()
engine = create_engine(config.get_postgres_uri())
get_session = sessionmaker(bind=engine)
app = Flask(__name__)


def is_valid(sku, batches):
    return sku in {b.sku for b in batches}


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    line = model.OrderLine(
        request.json["orderid"], request.json["sku"], request.json["qty"],
    )
    try:
        batchref = services.allocate(line, repo, session)

    except model.OutOfStock as e:
        return jsonify({"message": str(e)}), 400
    except services.InvalidSku as e:
        return jsonify({"message": str(e)}), 400
    return jsonify({"batchref": batchref}), 201


if __name__ == '__main__':
    app.run()
