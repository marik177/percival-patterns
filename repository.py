import abc

import sqlalchemy

import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        pass

    @abc.abstractmethod
    def add(self, batch: model.Batch):
        pass


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def get(self, reference):
        batch = self.session.execute(
            "SELECT reference, sku, _purchased_quantity, eta FROM batches "
            "WHERE reference=:ref",
            dict(ref=reference)
        ).fetchone()
        batch = model.Batch(*batch)
        orderlines_in_batch = (get_allocations(self.session, batch.reference))
        allocations = []
        for order_id in orderlines_in_batch:
            allocations.append(self.session.execute(
                "SELECT orderid, sku, qty FROM order_lines WHERE orderid=:order_id", dict(order_id=order_id)).fetchone(),
            )
        for orerline in allocations:
            batch._allocations.add(model.OrderLine(*orerline))
        return batch

    def add(self, batch: model.Batch):
        self.session.execute(
            "INSERT INTO batches (reference, sku, _purchased_quantity, eta) VALUES"
            "(:reference, :sku, :purchased_quantity, :eta)",
            dict(reference=batch.reference, sku=batch.sku,
                 purchased_quantity=batch._purchased_quantity, eta=batch.eta)
        )


def get_allocations(session, batchid):
    rows = list(
        session.execute(
            "SELECT orderid"
            " FROM allocations"
            " JOIN order_lines ON allocations.orderline_id = order_lines.id"
            " JOIN batches ON allocations.batch_id = batches.id"
            " WHERE batches.reference = :batchid",
            dict(batchid=batchid),
        )
    )
    return {row[0] for row in rows}
