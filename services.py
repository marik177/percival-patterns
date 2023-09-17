from repository import AbstractRepository
import model
import orm


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def allocate(line: model.OrderLine, repo: AbstractRepository, session) -> str:
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    batchref = model.allocate(line, batches)
    session.commit()
    return batchref


def deallocate(batch: model.Batch, line: model.OrderLine, repo: AbstractRepository, session):
    batch = repo.get(batch.reference)
    batch.deallocate(line)
    session.commit()
    return batch.reference


def add_batch(reference, sku, _purchased_quantity, eta, repo, session):
    batch = model.Batch(reference, sku, _purchased_quantity, eta)
    repo.add(batch)
    session.commit()


def get_batch_and_line_from_orderid_and_sku(orderid, sku, session):
    line = session.query(model.OrderLine).filter_by(orderid=orderid, sku=sku).one()
    batch_id = session.query(orm.allocations).filter_by(orderline_id=line.id).one().batch_id
    batch = session.query(orm.batches).filter_by(id=batch_id).one()
    return batch, line
