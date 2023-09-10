import abc

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
                "SELECT orderid, sku, qty FROM order_lines WHERE orderid=:order_id",
                dict(order_id=order_id)).fetchone(),
                               )
        for oderline in allocations:
            batch._allocations.add(model.OrderLine(*oderline))
        return batch

    def add(self, batch: model.Batch):
        self.session.execute(
            "INSERT INTO batches (reference, sku, _purchased_quantity, eta) VALUES"
            "(:reference, :sku, :purchased_quantity, :eta)",
            dict(reference=batch.reference, sku=batch.sku,
                 purchased_quantity=batch._purchased_quantity, eta=batch.eta)
        )

        if batch._allocations:
            batch_id = get_batch_id(self.session, batch.reference)
            for order_line in batch._allocations:
                self.session.execute(
                    "INSERT OR IGNORE INTO order_lines (orderid, sku, qty) VALUES "
                    "(:orderid, :sku, :qty)",
                    dict(orderid=order_line.orderid, sku=order_line.sku, qty=order_line.qty)

                )
                order_line_id = get_order_line_id(self.session, order_line.orderid, order_line.sku)

                self.session.execute(
                    "INSERT OR IGNORE INTO allocations (orderline_id, batch_id) VALUES"
                    "(:orderline_id, :batch_id)",
                    dict(orderline_id=order_line_id, batch_id=batch_id)
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


def get_batch_id(session, batch_ref):
    batch_id = session.execute(
        "SELECT id FROM batches WHERE reference=:batch_ref",
        dict(batch_ref=batch_ref)).fetchone()['id']

    return batch_id


def get_order_line_id(session, orderid, sku):
    order_line_id = session.execute(
        "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku",
        dict(orderid=orderid, sku=sku)
    ).fetchone()['id']
    return order_line_id
