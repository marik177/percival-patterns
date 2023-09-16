from flask import Flask, request, jsonify
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


def is_valid(sku, batches):
    return sku in {b.sku for b in batches}


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    session = get_session()
    batches = repository.SqlAlchemyRepository(session).list()
    line = model.OrderLine(
        request.json["orderid"], request.json["sku"], request.json["qty"],
    )
    if not is_valid(line.sku, batches):
        return jsonify({'message': f"Invalid sku {line.sku}"}), 400
    try:
        batchref = model.allocate(line, batches)
    except model.OutOfStock as e:
        return jsonify({"message": str(e)}), 400
    session.commit()
    return jsonify({"batchref": batchref}), 201


if __name__ == '__main__':
    app.run()
